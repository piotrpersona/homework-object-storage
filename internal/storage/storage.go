package storage

import (
	"bytes"
	"context"
	"fmt"
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
	bucketName                 = "example"
	workerContainerNamePattern = "amazin-object-storage-node-"
	dockerNetwork              = "homework-object-storage_amazin-object-storage"
	accessKeyEnv               = "MINIO_ACCESS_KEY"
	secretKeyEnv               = "MINIO_SECRET_KEY"
)

type Storage interface {
	Put(ctx context.Context, id string, body []byte) (err error)
	Get(ctx context.Context, id string) (body []byte, err error)
}

type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
}

type minioStorage struct {
	cli *minio.Client
}

func NewMinioStorage(cfg *Config) (s Storage, err error) {
	cli, err := minio.NewWithOptions(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: false,
	})
	if err != nil {
		err = fmt.Errorf("cannot create storage instance, err: %w", err)
		return
	}
	s = &minioStorage{
		cli: cli,
	}
	return
}

func (s *minioStorage) Put(ctx context.Context, id string, body []byte) (err error) {
	_, err = s.cli.PutObjectWithContext(ctx, bucketName, id, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{})
	if err != nil {
		err = fmt.Errorf("cannot put object '%s' into storage instance, err: %w", id, err)
		return
	}
	return
}

func (s *minioStorage) Get(ctx context.Context, id string) (body []byte, err error) {
	object, err := s.cli.GetObjectWithContext(ctx, bucketName, id, minio.GetObjectOptions{})
	if err != nil {
		err = fmt.Errorf("cannot get object '%s', err: %w", id, err)
		return
	}
	_, err = object.Read(body)
	if err != nil {
		err = fmt.Errorf("cannot read object '%s' body, err: %w", id, err)
		return
	}
	return
}

type balancedStorage struct {
	cli *dockercli.Client
}

func NewBalancedStorage(cli *dockercli.Client) Storage {
	return &balancedStorage{
		cli: cli,
	}
}

// consistent package doesn't provide a default hashing function.
// You should provide a proper one to distribute keys/members uniformly.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
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

func (s *balancedStorage) Put(ctx context.Context, id string, body []byte) (err error) {
	containers, err := s.cli.ContainerList(ctx, types.ContainerListOptions{
		Filters: filters.NewArgs(filters.KeyValuePair{
			Key: "status", Value: "running",
		}),
	})
	if err != nil {
		err = fmt.Errorf("cannot list containers, err: %w", err)
		return
	}

	storageNodes := make(map[string]storageNode, len(containers))
containerLoop:
	for _, container := range containers {
		node := storageNode{
			ID: container.ID,
		}
		if container.NetworkSettings == nil {
			continue containerLoop
		}
		node.Endpoint = fmt.Sprintf("%s:%d", container.NetworkSettings.Networks[dockerNetwork].IPAddress, 9000)

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

	cfg := consistent.Config{
		PartitionCount:    len(storageNodes),
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	c := consistent.New(nil, cfg)
	for _, node := range storageNodes {
		c.Add(node)
	}

	nodeID := c.LocateKey([]byte(id))
	node := storageNodes[nodeID.String()]
	fmt.Printf("node: %s %s\n", node.Endpoint, node.Name)
	return
}

func (s *balancedStorage) Get(ctx context.Context, id string) (body []byte, err error) {
	return
}
