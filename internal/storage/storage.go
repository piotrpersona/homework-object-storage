package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	dockercli "github.com/docker/docker/client"
	"github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/credentials"
)

const (
	workerContainerNamePattern = "amazin-object-storage-node-"
	dockerNetwork              = "homework-object-storage_amazin-object-storage"
	accessKeyEnv               = "MINIO_ACCESS_KEY"
	secretKeyEnv               = "MINIO_SECRET_KEY"
	minioWorkerPort            = 9000
)

type Storage interface {
	Setup(ctx context.Context) (err error)
	Put(ctx context.Context, id, contentType string, body []byte) (err error)
	Get(ctx context.Context, id string) (body []byte, contentType string, err error)
}

type minioConfig struct {
	Endpoint   string
	AccessKey  string
	SecretKey  string
	BucketName string
}

type minioStorage struct {
	cli        *minio.Client
	bucketName string
}

func newMinioStorage(cfg *minioConfig) (s Storage, err error) {
	cli, err := minio.NewWithOptions(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: false,
	})
	if err != nil {
		err = fmt.Errorf("cannot create storage instance, err: %w", err)
		return
	}
	s = &minioStorage{
		cli:        cli,
		bucketName: cfg.BucketName,
	}
	return
}

func (s *minioStorage) Setup(ctx context.Context) (err error) {
	exists, bucketExistsErr := s.cli.BucketExists(s.bucketName)
	if bucketExistsErr == nil && exists {
		log.Printf("We already own %s\n", s.bucketName)
		return
	}
	if bucketExistsErr != nil {
		err = fmt.Errorf("cannot get bucket info, err: %w", bucketExistsErr)
		return
	}
	makeErr := s.cli.MakeBucket(s.bucketName, "")
	if makeErr == nil {
		log.Printf("Successfully created bucket %s\n", s.bucketName)
		return
	}
	err = fmt.Errorf("cannot create bucket %s, err: %w", s.bucketName, makeErr)
	return
}

func (s *minioStorage) Put(ctx context.Context, id, contentType string, body []byte) (err error) {
	_, err = s.cli.PutObjectWithContext(ctx, s.bucketName, id, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		err = fmt.Errorf("cannot put object '%s' into storage instance, err: %w", id, err)
		return
	}
	return
}

func (s *minioStorage) Get(ctx context.Context, id string) (body []byte, contentType string, err error) {
	object, err := s.cli.GetObjectWithContext(ctx, s.bucketName, id, minio.GetObjectOptions{})
	if err != nil {
		err = fmt.Errorf("cannot get object '%s', err: %w", id, err)
		return
	}
	info, err := object.Stat()
	if err != nil {
		err = fmt.Errorf("cannot get object stats, err: %w", err)
		return
	}
	contentType = info.ContentType
	body, err = io.ReadAll(object)
	if err != nil {
		err = fmt.Errorf("cannot read object '%s' body, err: %w", id, err)
		return
	}
	return
}

type balancedStorage struct {
	cli        *dockercli.Client
	bucketName string
}

func NewBalancedStorage(cli *dockercli.Client, bucketName string) Storage {
	return &balancedStorage{
		cli:        cli,
		bucketName: bucketName,
	}
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type storageNode struct {
	ID        string
	Name      string
	Endpoint  string
	AccessKey string
	SecretKey string
}

func (n storageNode) String() string {
	return fmt.Sprintf("%s.%s", n.ID, n.Name)
}

func (s balancedStorage) getStorageNodes(ctx context.Context) (storageNodes map[string]storageNode, err error) {
	containers, err := s.cli.ContainerList(ctx, types.ContainerListOptions{
		Filters: filters.NewArgs(filters.KeyValuePair{
			Key: "status", Value: "running",
		}),
	})
	if err != nil {
		err = fmt.Errorf("cannot list containers, err: %w", err)
		return
	}

	storageNodes = make(map[string]storageNode, len(containers))
containerLoop:
	for _, container := range containers {
		node := storageNode{
			ID: container.ID,
		}
		if container.NetworkSettings == nil {
			continue containerLoop
		}
		node.Endpoint = fmt.Sprintf("%s:%d", container.NetworkSettings.Networks[dockerNetwork].IPAddress, minioWorkerPort)

		inspectData, inspectErr := s.cli.ContainerInspect(ctx, container.ID)
		if inspectErr != nil {
			err = fmt.Errorf("cannot inspect container %s: %w", container.ID, inspectErr)
			return
		}
		containerEnv := make(map[string]string)
		for _, env := range inspectData.Config.Env {
			splitted := strings.Split(env, "=")
			if len(splitted) > 1 {
				containerEnv[splitted[0]] = splitted[1]
			}
		}
		node.AccessKey = containerEnv[accessKeyEnv]
		node.SecretKey = containerEnv[secretKeyEnv]
		if len(container.Names) > 0 {
			node.Name = container.Names[0]
		}
		if strings.Contains(node.Name, workerContainerNamePattern) {
			storageNodes[node.String()] = node
		}
	}
	return
}

func (s *balancedStorage) registerStorageNodes(storageNodes map[string]storageNode) (c *consistent.Consistent) {
	cfg := consistent.Config{
		PartitionCount:    len(storageNodes),
		ReplicationFactor: 0,
		Load:              1.25,
		Hasher:            hasher{},
	}
	c = consistent.New(nil, cfg)
	for _, node := range storageNodes {
		c.Add(node)
	}
	return
}

func (s *balancedStorage) getStorageWorker(ctx context.Context, id string) (storage Storage, storageID string, err error) {
	storageNodes, err := s.getStorageNodes(ctx)
	if err != nil {
		return
	}
	consistent := s.registerStorageNodes(storageNodes)
	storageID = consistent.LocateKey([]byte(id)).String()
	node := storageNodes[storageID]
	storage, err = newMinioStorage(&minioConfig{
		Endpoint:   node.Endpoint,
		AccessKey:  node.AccessKey,
		SecretKey:  node.SecretKey,
		BucketName: s.bucketName,
	})
	if err != nil {
		return
	}
	if err = storage.Setup(ctx); err != nil {
		return
	}
	if err != nil {
		err = fmt.Errorf("cannot get storage %s, err: %w", storageID, err)
		return
	}
	log.Printf("using storage worker '%s' for object '%s'\n", storageID, id)
	return
}

func (s *balancedStorage) Setup(ctx context.Context) (err error) {
	nodes, err := s.getStorageNodes(ctx)
	if err != nil {
		return
	}
	for _, node := range nodes {
		storage, createStorageErr := newMinioStorage(&minioConfig{
			Endpoint:   node.Endpoint,
			AccessKey:  node.AccessKey,
			SecretKey:  node.SecretKey,
			BucketName: s.bucketName,
		})
		if createStorageErr != nil {
			err = fmt.Errorf("cannot create minio storage %s, err: %w", node.String(), createStorageErr)
			return
		}
		err = storage.Setup(ctx)
		if err != nil {
			err = fmt.Errorf("cannot setup storage %s, err: %w", node.String(), err)
			return
		}
	}
	return
}

func (s *balancedStorage) Put(ctx context.Context, id, contentType string, body []byte) (err error) {
	storage, storageID, err := s.getStorageWorker(ctx, id)
	if err != nil {
		return
	}
	err = storage.Put(ctx, id, contentType, body)
	if err != nil {
		err = fmt.Errorf("cannot put using worker '%s', err: %w", storageID, err)
	}
	return
}

func (s *balancedStorage) Get(ctx context.Context, id string) (body []byte, contentType string, err error) {
	storage, storageID, err := s.getStorageWorker(ctx, id)
	if err != nil {
		return
	}
	body, contentType, err = storage.Get(ctx, id)
	if err != nil {
		err = fmt.Errorf("cannot get using worker '%s', err: %w", storageID, err)
	}
	return
}
