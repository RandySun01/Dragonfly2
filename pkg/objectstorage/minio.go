/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package objectstorage

import (
	"bytes"
	"context"
	"fmt"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"
)

type Minio struct {
	// Minio client.
	client *minio.Client

	// region is storage region.
	region string

	// endpoint is datacenter endpoint.
	endpoint string
}

// New Minio instance.
func newMinio(region, endpoint, accessKey, secretKey string, httpClient *http.Client) (ObjectStorage, error) {

	minioClient, err := minio.New(
		endpoint,
		&minio.Options{
			Creds:     credentials.NewStaticV4(accessKey, secretKey, ""),
			Secure:    true,
			Region:    region,
			Transport: httpClient.Transport,
		},
	)

	if err != nil {
		return nil, fmt.Errorf("new Minio session failed: %s", err)
	}
	fmt.Println("minio init minio success", region, endpoint)
	return &Minio{
		client:   minioClient,
		region:   region,
		endpoint: endpoint,
	}, nil
}

// GetMetadata returns metadata of object storage.
func (m *Minio) GetMetadata(ctx context.Context) *Metadata {
	return &Metadata{
		Name:     ServiceNameOSS,
		Region:   m.region,
		Endpoint: m.endpoint,
	}
}

// GetBucketMetadata returns metadata of bucket.
func (m *Minio) GetBucketMetadata(ctx context.Context, bucketName string) (*BucketMetadata, error) {
	_, err := m.client.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	return &BucketMetadata{
		Name: bucketName,
	}, nil
}

// CreateBucket creates bucket of object storage.
func (m *Minio) CreateBucket(ctx context.Context, bucketName string) error {

	err := m.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{Region: "us-east-1"})
	return err
}

// DeleteBucket deletes bucket of object storage.
func (m *Minio) DeleteBucket(ctx context.Context, bucketName string) error {
	err := m.client.RemoveBucket(ctx, bucketName)
	return err
}

// ListBucketMetadatas get list bucket of object storage.
func (m *Minio) ListBucketMetadatas(ctx context.Context) ([]*BucketMetadata, error) {
	resp, err := m.client.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	var metadatas []*BucketMetadata
	for _, bucket := range resp {
		metadatas = append(metadatas, &BucketMetadata{
			Name:     bucket.Name,
			CreateAt: bucket.CreationDate,
		})
	}
	return metadatas, nil
}

// GetObjectMetadata returns metadata of object.
func (m *Minio) GetObjectMetadata(ctx context.Context, bucketName, objectKey string) (*ObjectMetadata, bool, error) {

	objStat, err := m.client.StatObject(context.Background(), bucketName, objectKey, minio.StatObjectOptions{})
	if err != nil {
		return nil, false, err
	}
	return &ObjectMetadata{
		Key:                objectKey,
		ContentDisposition: objStat.Metadata.Get("Content-Disposition"),
		ContentEncoding:    objStat.Metadata.Get("Content-Encoding"),
		ContentLanguage:    objStat.Metadata.Get("Content-Language"),
		ContentLength:      objStat.Size,
		ContentType:        objStat.ContentType,
		ETag:               objStat.ETag,
		Digest:             objStat.UserMetadata[MetaDigest],
		LastModifiedTime:   objStat.LastModified,
		StorageClass:       objStat.StorageClass,
	}, true, nil
}

// GetObjectMetadatas returns the metadatas of the objects.
func (m *Minio) GetObjectMetadatas(ctx context.Context, bucketName, prefix, marker, delimiter string, limit int64) (*ObjectMetadatas, error) {

	opt := minio.ListObjectsOptions{
		UseV1:      true,
		Prefix:     prefix,
		MaxKeys:    int(limit),
		StartAfter: marker,
	}
	opt.Set("delimiter", delimiter)
	objectInfo := m.client.ListObjects(ctx, bucketName, opt)

	metadatas := make([]*ObjectMetadata, 0, 1024)
	for content := range objectInfo {
		metadatas = append(metadatas, &ObjectMetadata{
			Key:              content.Key,
			ContentLength:    content.Size,
			ETag:             content.ETag,
			LastModifiedTime: content.LastModified,
			StorageClass:     content.StorageClass,
		})
	}

	commonPrefixes := make([]string, 0, 1024)
	return &ObjectMetadatas{
		Metadatas:      metadatas,
		CommonPrefixes: commonPrefixes,
	}, nil
}

// GetOject returns data of object.
func (m *Minio) GetOject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error) {
	object, err := m.client.GetObject(ctx, bucketName, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return object, nil
}

// PutObject puts data of object.
func (m *Minio) PutObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error {
	userMetadata := map[string]string{}
	userMetadata[MetaDigest] = digest

	opts := minio.PutObjectOptions{ContentType: "application/octet-stream", UserMetadata: userMetadata}
	// 获取文件长度
	buf := &bytes.Buffer{}
	objectSize, err := io.Copy(buf, reader)
	if err != nil {
		return err
	}
	_, err = m.client.PutObject(ctx, bucketName, objectKey, buf, objectSize, opts)
	return err
}

// DeleteObject deletes data of object.
func (m *Minio) DeleteObject(ctx context.Context, bucketName, objectKey string) error {
	opts := minio.RemoveObjectOptions{}
	err := m.client.RemoveObject(ctx, bucketName, objectKey, opts)

	return err
}

// IsObjectExist returns whether the object exists.
func (m *Minio) IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error) {
	_, isExist, err := m.GetObjectMetadata(ctx, bucketName, objectKey)
	if err != nil {
		return false, err
	}

	if !isExist {
		return false, nil
	}

	return true, nil
}

// IsBucketExist returns whether the bucket exists.
func (m *Minio) IsBucketExist(ctx context.Context, bucketName string) (bool, error) {
	return m.client.BucketExists(ctx, bucketName)
}

// CopyObject copy object from source to destination.
func (m *Minio) CopyObject(ctx context.Context, bucketName, sourceObjectKey, destinationObjectKey string) error {
	sourceObjectKey = path.Join(bucketName, sourceObjectKey)
	// Destination object
	dstOpts := minio.CopyDestOptions{
		Bucket: bucketName,
		Object: destinationObjectKey,
	}
	srcOpts := minio.CopySrcOptions{
		Bucket: bucketName,
		Object: sourceObjectKey,
	}
	_, err := m.client.CopyObject(ctx, dstOpts, srcOpts)
	return err
}

// GetSignURL returns sign url of object.
func (m *Minio) GetSignURL(ctx context.Context, bucketName, objectKey string, method Method, expire time.Duration) (string, error) {
	var req *url.URL
	reqParams := make(url.Values)
	reqParams.Set("response-content-disposition", fmt.Sprintf("attachment; filename=%s", objectKey))
	switch method {
	case MethodGet:
		req, _ = m.client.PresignedGetObject(ctx, bucketName, objectKey, expire, reqParams)
	case MethodPut:
		req, _ = m.client.PresignedPutObject(ctx, bucketName, objectKey, expire)
	case MethodHead:
		req, _ = m.client.PresignedHeadObject(ctx, bucketName, objectKey, expire, reqParams)
	case MethodDelete:
		return "", fmt.Errorf("not support method %s", method)
	case MethodList:
		return "", fmt.Errorf("not support method %s", method)
	default:
		return "", fmt.Errorf("not support method %s", method)
	}

	return req.String(), nil
}

// getStorageClass returns the default storage class if the input is empty.
func (m *Minio) getStorageClass(storageClass *string) *string {
	if storageClass == nil || *storageClass == "" {
		storageClass := awss3.StorageClassStandard
		return &storageClass
	}
	return storageClass
}
