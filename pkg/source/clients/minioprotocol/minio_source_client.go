/*
 *     Copyright 2020 The Dragonfly Authors
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

package minioprotocol

import (
	"errors"
	"fmt"
	"github.com/go-http-utils/headers"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"net/http"
	"net/textproto"
	"net/url"
	"path/filepath"
	"strings"
	"sync"

	"d7y.io/dragonfly/v2/pkg/source"
	pkgstrings "d7y.io/dragonfly/v2/pkg/strings"
)

const MINIOClient = "minio"

const (
	endpoint        = "endpoint"
	accessKeyID     = "accessKeyID"
	accessKeySecret = "accessKeySecret"
	securityToken   = "securityToken"
)

var _ source.ResourceClient = (*minioSourceClient)(nil)

func init() {
	source.RegisterBuilder(MINIOClient, source.NewPlainResourceClientBuilder(Builder))
}

func Builder(optionYaml []byte) (source.ResourceClient, source.RequestAdapter, []source.Hook, error) {
	return NewMinioSourceClient(), adaptor, nil, nil
}

func adaptor(request *source.Request) *source.Request {
	clonedRequest := request.Clone(request.Context())
	if request.Header.Get(source.Range) != "" {
		clonedRequest.Header.Set(headers.Range, fmt.Sprintf("bytes=%s", request.Header.Get(source.Range)))
		clonedRequest.Header.Del(source.Range)
	}
	clonedRequest.URL.Path = strings.TrimPrefix(clonedRequest.URL.Path, "/")
	return clonedRequest
}

func NewMinioSourceClient(opts ...MINIOSourceClientOption) source.ResourceClient {
	return newMINIOSourceClient(opts...)
}

func newMINIOSourceClient(opts ...MINIOSourceClientOption) source.ResourceClient {
	sourceClient := &minioSourceClient{
		clientMap: sync.Map{},
		accessMap: sync.Map{},
	}
	for i := range opts {
		opts[i](sourceClient)
	}
	return sourceClient
}

type MINIOSourceClientOption func(p *minioSourceClient)

// minioSourceClient is an implementation of the interface of source.ResourceClient.
type minioSourceClient struct {
	// endpoint_accessKeyID_accessKeySecret -> minioClient
	clientMap sync.Map
	accessMap sync.Map
}

func (osc *minioSourceClient) GetContentLength(request *source.Request) (int64, error) {
	client, err := osc.getClient(request.Header)
	fmt.Printf("GetContentLength get minio bucket info host:%s, path: %s\n", request.URL.Host, request.URL.Path)
	if err != nil {
		return source.UnknownSourceFileLen, err
	}
	exists, err := client.BucketExists(request.Context(), request.URL.Host)
	if err != nil {
		return source.UnknownSourceFileLen, fmt.Errorf("get minio bucket %s: %w", request.URL.Host, err)
	}
	if !exists {
		return source.UnknownSourceFileLen, fmt.Errorf("get minio bucket %s: %w", request.URL.Host, err)
	}
	objectInfo, err := client.StatObject(request.Context(), request.URL.Host, request.URL.Path, getOptions(request.Header))

	if err != nil {
		return source.UnknownSourceFileLen, fmt.Errorf("get minio object %s meta: %w", request.URL.Path, err)
	}
	return objectInfo.Size, nil
}

func (osc *minioSourceClient) IsSupportRange(request *source.Request) (bool, error) {
	fmt.Printf("IsSupportRange get minio bucket info host:%s, path:%s\n", request.URL.Host, request.URL.Path)
	if request.Header.Get(headers.Range) == "" {
		request.Header.Set(headers.Range, "bytes=0-0")
	}
	client, err := osc.getClient(request.Header)

	if err != nil {
		return false, fmt.Errorf("get minio client: %w", err)
	}
	exists, err := client.BucketExists(request.Context(), request.URL.Host)
	if err != nil {
		return false, fmt.Errorf("get minio bucket %s: %w", request.URL.Host, err)
	}
	if !exists {
		return false, fmt.Errorf("get minio bucket not exists %s: %w", request.URL.Host, err)
	}
	_, err = client.StatObject(request.Context(), request.URL.Path, request.URL.Path, getOptions(request.Header))
	if err != nil {
		return false, err
	}

	return true, nil
}

func (osc *minioSourceClient) GetMetadata(request *source.Request) (*source.Metadata, error) {
	fmt.Printf("GetMetadata get minio bucket info host:%s, path:%s\n", request.URL.Host, request.URL.Path)

	request = request.Clone(request.Context())
	request.Header.Set(headers.Range, "bytes=0-0")

	client, err := osc.getClient(request.Header)
	if err != nil {
		return nil, fmt.Errorf("get minio client: %w", err)
	}

	exists, err := client.BucketExists(request.Context(), request.URL.Host)
	if err != nil {
		return nil, fmt.Errorf("get minio bucket %s: %w", request.URL.Host, err)
	}
	if !exists {
		return nil, fmt.Errorf("get minio bucket not exists %s: %w", request.URL.Host, err)
	}

	objectInfo, err := client.StatObject(request.Context(), request.URL.Host, request.URL.Path, getOptions(request.Header))
	if err != nil {
		return nil, fmt.Errorf("get minio object %s meta: %w", request.URL.Path, err)
	}

	hdr := source.Header{}
	for k, v := range objectInfo.Metadata {
		if len(v) > 0 {
			hdr.Set(k, v[0])
		}
	}
	return &source.Metadata{
		Header:             hdr,
		Status:             http.StatusText(http.StatusOK),
		StatusCode:         http.StatusOK,
		SupportRange:       true,
		TotalContentLength: objectInfo.Size,
		Validate: func() error {
			return nil
		},
		Temporary: true,
	}, nil
}

func (msc *minioSourceClient) IsExpired(request *source.Request, info *source.ExpireInfo) (bool, error) {
	fmt.Printf("IsExpired get minio bucket info host:%s, path:%s\n", request.URL.Host, request.URL.Path)

	client, err := msc.getClient(request.Header)
	if err != nil {
		return false, fmt.Errorf("get minio client: %w", err)
	}
	exists, err := client.BucketExists(request.Context(), request.URL.Host)
	if err != nil {
		return false, fmt.Errorf("get minio bucket %s: %w", request.URL.Host, err)
	}
	if !exists {
		return false, fmt.Errorf("get minio bucket not exists %s: %w", request.URL.Host, err)
	}

	objectInfo, err := client.StatObject(request.Context(), request.URL.Host, request.URL.Path, getOptions(request.Header))
	if err != nil {
		return false, fmt.Errorf("get minio object %s meta: %w", request.URL.Path, err)
	}

	return !(objectInfo.ETag == info.ETag || objectInfo.LastModified.Format(source.TimeFormat) == info.LastModified), nil
}

func (msc *minioSourceClient) Download(request *source.Request) (*source.Response, error) {
	fmt.Printf("Download get minio bucket info host:%s, path:%s\n", request.URL.Host, request.URL.Path)

	client, err := msc.getClient(request.Header)
	if err != nil {
		return nil, fmt.Errorf("get minio client: %w", err)
	}
	exists, err := client.BucketExists(request.Context(), request.URL.Host)
	if err != nil {
		return nil, fmt.Errorf("get minio bucket %s: %w", request.URL.Host, err)
	}
	if !exists {
		return nil, fmt.Errorf("get minio bucket not exists %s: %w", request.URL.Host, err)
	}

	objectResult, err := client.GetObject(request.Context(), request.URL.Host, request.URL.Path, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get minio Object %s: %w", request.URL.Path, err)
	}
	objectInfo, err := objectResult.Stat()
	if err != nil {
		return nil, fmt.Errorf("get minio objectInfo %s: %w", request.URL.Path, err)
	}
	response := source.NewResponse(
		objectResult,
		source.WithExpireInfo(
			source.ExpireInfo{
				LastModified: objectInfo.LastModified.Format(source.TimeFormat),
				ETag:         objectInfo.ETag,
			},
		))
	response.ContentLength = objectInfo.Size
	return response, nil
}

func (msc *minioSourceClient) GetLastModified(request *source.Request) (int64, error) {
	fmt.Printf("GetLastModified get minio bucket info host:%s, path:%s\n", request.URL.Host, request.URL.Path)

	client, err := msc.getClient(request.Header)
	if err != nil {
		return -1, fmt.Errorf("get minio client: %w", err)
	}
	exists, err := client.BucketExists(request.Context(), request.URL.Host)
	if err != nil {
		return -1, fmt.Errorf("get minio bucket %s: %w", request.URL.Host, err)
	}
	if !exists {
		return -1, fmt.Errorf("get minio bucket not exists %s: %w", request.URL.Host, err)
	}

	objectInfo, err := client.StatObject(request.Context(), request.URL.Host, request.URL.Path, getOptions(request.Header))
	if err != nil {
		return -1, fmt.Errorf("get minio object %s meta: %w", request.URL.Path, err)
	}
	lastModified := objectInfo.LastModified

	return lastModified.UnixMilli(), nil
}

func (msc *minioSourceClient) List(request *source.Request) (urls []source.URLEntry, err error) {
	fmt.Printf("List get minio bucket info host:%s, path:%s\n", request.URL.Host, request.URL.Path)

	client, err := msc.getClient(request.Header)
	if err != nil {
		return nil, fmt.Errorf("get oss client: %w", err)
	}
	exists, err := client.BucketExists(request.Context(), request.URL.Host)
	if err != nil {
		return nil, fmt.Errorf("get minio bucket %s: %w", request.URL.Host, err)
	}
	if !exists {
		return nil, fmt.Errorf("get minio bucket not exists %s: %w", request.URL.Host, err)
	}

	isDir, err := msc.isDirectory(client, request)
	if err != nil {
		return nil, err
	}
	// if request is a single file, just return
	if !isDir {
		return []source.URLEntry{buildURLEntry(false, request.URL)}, nil
	}
	// list all files and subdirectory
	path := addTrailingSlash(request.URL.Path)

	opt := minio.ListObjectsOptions{Prefix: path, UseV1: true}
	objectsCh := client.ListObjects(request.Context(), request.URL.Host, opt)

	for object := range objectsCh {
		if object.Key != opt.Prefix {
			url := *request.URL
			url.Path = addLeadingSlash(object.Key)
			urls = append(urls, buildURLEntry(false, &url))
		}
		url := *request.URL
		url.Path = addLeadingSlash(object.Key)
		urls = append(urls, buildURLEntry(true, &url))
	}

	return urls, nil
}

func (msc *minioSourceClient) isDirectory(client *minio.Client, request *source.Request) (bool, error) {
	uPath := addTrailingSlash(request.URL.Path)

	fmt.Printf("isDirectory get minio bucket info host:%s, path:%s, uPath:%s\n", request.URL.Host, request.URL.Path, uPath)

	objectCh := client.ListObjects(request.Context(), request.URL.Host, minio.ListObjectsOptions{Prefix: uPath, MaxKeys: 1})
	for object := range objectCh {
		if object.Err != nil {
			return false, object.Err
		}
		// 如果有任何对象，那么该路径不是一个目录
		if object.Key != uPath {
			return false, nil
		}
	}
	// 如果没有对象，那么该路径是一个目录
	return true, nil
}

func (msc *minioSourceClient) getClient(header source.Header) (*minio.Client, error) {
	endpoint := header.Get(endpoint)
	if pkgstrings.IsBlank(endpoint) {
		return nil, errors.New("endpoint is empty")
	}
	accessKeyID := header.Get(accessKeyID)
	if pkgstrings.IsBlank(accessKeyID) {
		return nil, errors.New("accessKeyID is empty")
	}
	accessKeySecret := header.Get(accessKeySecret)
	if pkgstrings.IsBlank(accessKeySecret) {
		return nil, errors.New("accessKeySecret is empty")
	}
	securityToken := header.Get(securityToken)
	if !pkgstrings.IsBlank(securityToken) {
		return minio.New(endpoint,
			&minio.Options{
				Creds:  credentials.NewStaticV4(accessKeyID, accessKeySecret, securityToken),
				Secure: true,
			})
	}
	clientKey := buildClientKey(endpoint, accessKeyID, accessKeySecret)
	if client, ok := msc.clientMap.Load(clientKey); ok {
		return client.(*minio.Client), nil
	}

	client, err := minio.New(endpoint,
		&minio.Options{
			Creds:  credentials.NewStaticV4(accessKeyID, accessKeySecret, ""),
			Secure: true,
		})

	if err != nil {
		return nil, err
	}
	actual, _ := msc.clientMap.LoadOrStore(clientKey, client)
	return actual.(*minio.Client), nil
}

func buildClientKey(endpoint, accessKeyID, accessKeySecret string) string {
	return fmt.Sprintf("%s_%s_%s", endpoint, accessKeyID, accessKeySecret)
}

func getOptions(header source.Header) minio.StatObjectOptions {
	opts := minio.StatObjectOptions{}
	for key, values := range header {
		if key == textproto.CanonicalMIMEHeaderKey(endpoint) || key == textproto.CanonicalMIMEHeaderKey(accessKeyID) || key == textproto.CanonicalMIMEHeaderKey(accessKeySecret) {
			continue
		}
		// minio-http Header value must be string type, while http header value is  minio.StatObjectOptions.Header
		// according to HTTP RFC2616 Multiple message-header fields with the same field-name MAY be present in a message
		// if and only if the entire field-value for that header field is defined as a comma-separated list
		value := strings.Join(values, ",")
		opts.Set(key, value)
	}
	return opts
}

func buildURLEntry(isDir bool, url *url.URL) source.URLEntry {
	if isDir {
		url.Path = addTrailingSlash(url.Path)
		list := strings.Split(url.Path, "/")
		return source.URLEntry{URL: url, Name: list[len(list)-2], IsDir: true}
	}
	_, name := filepath.Split(url.Path)
	return source.URLEntry{URL: url, Name: name, IsDir: false}
}

func addLeadingSlash(s string) string {
	if strings.HasPrefix(s, "/") {
		return s
	}
	return "/" + s
}

func addTrailingSlash(s string) string {
	if strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
}
