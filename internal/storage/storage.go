package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/minio/minio-go"
	"github.com/minio/minio-go/pkg/credentials"
)

const bucketName = "example"

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
