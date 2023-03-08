package storage

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
	"github.com/docker/docker/api/types"
	dockercli "github.com/docker/docker/client"
	"github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/credentials"
)

const (
	bucketName                 = "example"
	workerContainerNamePattern = "amazin-object-storage-node-"
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
	ID   string
	Name string
}

func (n storageNode) String() string {
	return fmt.Sprintf("%s.%s", n.ID, n.Name)
}

func (s *balancedStorage) Put(ctx context.Context, id string, body []byte) (err error) {
	containers, err := s.cli.ContainerList(ctx, types.ContainerListOptions{})
	if err != nil {
		err = fmt.Errorf("cannot list containers, err: %w", err)
		return
	}

	storageNodes := make([]storageNode, 0, len(containers))
containerLoop:
	for _, container := range containers {
		for _, name := range container.Names {
			if strings.Contains(name, workerContainerNamePattern) {
				storageNodes = append(storageNodes, storageNode{
					ID:   container.ID,
					Name: name,
				})
				continue containerLoop
			}
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

	member := c.LocateKey([]byte(id))
	fmt.Printf("member: %s\n", member)
	return
}

func (s *balancedStorage) Get(ctx context.Context, id string) (body []byte, err error) {
	return
}
