/*
Copyright 2023 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cloud

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/fsx"
	"github.com/aws/aws-sdk-go-v2/service/fsx/types"
	"github.com/golang/mock/gomock"
	"github.com/kubernetes-sigs/aws-fsx-openzfs-csi-driver/pkg/cloud/mocks"
)

func TestTagResource(t *testing.T) {
	testCases := []struct {
		name       string
		resourceId string
		tags       map[string]string
		setupMock  func(*mocks.MockFSx)
		expError   bool
	}{
		{
			name:       "success - tag filesystem",
			resourceId: "fs-12345",
			tags: map[string]string{
				"env":  "test",
				"team": "dev",
			},
			setupMock: func(m *mocks.MockFSx) {
				m.EXPECT().DescribeFileSystems(gomock.Any(), gomock.Any()).Return(
					&fsx.DescribeFileSystemsOutput{
						FileSystems: []types.FileSystem{
							{
								FileSystemId: aws.String("fs-12345"),
								ResourceARN:  aws.String("arn:aws:fsx:us-east-1:123456789012:file-system/fs-12345"),
							},
						},
					}, nil)
				m.EXPECT().TagResource(gomock.Any(), &fsx.TagResourceInput{
					ResourceARN: aws.String("arn:aws:fsx:us-east-1:123456789012:file-system/fs-12345"),
					Tags: []types.Tag{
						{Key: aws.String("env"), Value: aws.String("test")},
						{Key: aws.String("team"), Value: aws.String("dev")},
					},
				}).Return(&fsx.TagResourceOutput{}, nil)
			},
		},
		{
			name:       "success - tag volume",
			resourceId: "fsvol-12345",
			tags: map[string]string{
				"project": "test-project",
			},
			setupMock: func(m *mocks.MockFSx) {
				m.EXPECT().DescribeVolumes(gomock.Any(), gomock.Any()).Return(
					&fsx.DescribeVolumesOutput{
						Volumes: []types.Volume{
							{
								VolumeId:    aws.String("fsvol-12345"),
								ResourceARN: aws.String("arn:aws:fsx:us-east-1:123456789012:volume/fsvol-12345"),
							},
						},
					}, nil)
				m.EXPECT().TagResource(gomock.Any(), &fsx.TagResourceInput{
					ResourceARN: aws.String("arn:aws:fsx:us-east-1:123456789012:volume/fsvol-12345"),
					Tags: []types.Tag{
						{Key: aws.String("project"), Value: aws.String("test-project")},
					},
				}).Return(&fsx.TagResourceOutput{}, nil)
			},
		},
		{
			name:       "fail - invalid resource ID",
			resourceId: "invalid-12345",
			tags:       map[string]string{"key": "value"},
			setupMock:  func(m *mocks.MockFSx) {},
			expError:   true,
		},
		{
			name:       "fail - filesystem not found",
			resourceId: "fs-12345",
			tags:       map[string]string{"key": "value"},
			setupMock: func(m *mocks.MockFSx) {
				m.EXPECT().DescribeFileSystems(gomock.Any(), gomock.Any()).Return(
					&fsx.DescribeFileSystemsOutput{FileSystems: []types.FileSystem{}}, nil)
			},
			expError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			mockFSx := mocks.NewMockFSx(mockCtrl)
			tc.setupMock(mockFSx)

			c := &cloud{
				fsx: mockFSx,
			}

			err := c.TagResource(context.TODO(), tc.resourceId, tc.tags)
			if tc.expError && err == nil {
				t.Fatal("Expected error, got none")
			}
			if !tc.expError && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}

func TestUntagResource(t *testing.T) {
	testCases := []struct {
		name       string
		resourceId string
		tagKeys    []string
		setupMock  func(*mocks.MockFSx)
		expError   bool
	}{
		{
			name:       "success - untag filesystem",
			resourceId: "fs-12345",
			tagKeys:    []string{"env", "team"},
			setupMock: func(m *mocks.MockFSx) {
				m.EXPECT().DescribeFileSystems(gomock.Any(), gomock.Any()).Return(
					&fsx.DescribeFileSystemsOutput{
						FileSystems: []types.FileSystem{
							{
								FileSystemId: aws.String("fs-12345"),
								ResourceARN:  aws.String("arn:aws:fsx:us-east-1:123456789012:file-system/fs-12345"),
							},
						},
					}, nil)
				m.EXPECT().UntagResource(gomock.Any(), &fsx.UntagResourceInput{
					ResourceARN: aws.String("arn:aws:fsx:us-east-1:123456789012:file-system/fs-12345"),
					TagKeys:     []string{"env", "team"},
				}).Return(&fsx.UntagResourceOutput{}, nil)
			},
		},
		{
			name:       "success - untag volume",
			resourceId: "fsvol-12345",
			tagKeys:    []string{"project"},
			setupMock: func(m *mocks.MockFSx) {
				m.EXPECT().DescribeVolumes(gomock.Any(), gomock.Any()).Return(
					&fsx.DescribeVolumesOutput{
						Volumes: []types.Volume{
							{
								VolumeId:    aws.String("fsvol-12345"),
								ResourceARN: aws.String("arn:aws:fsx:us-east-1:123456789012:volume/fsvol-12345"),
							},
						},
					}, nil)
				m.EXPECT().UntagResource(gomock.Any(), &fsx.UntagResourceInput{
					ResourceARN: aws.String("arn:aws:fsx:us-east-1:123456789012:volume/fsvol-12345"),
					TagKeys:     []string{"project"},
				}).Return(&fsx.UntagResourceOutput{}, nil)
			},
		},
		{
			name:       "fail - invalid resource ID",
			resourceId: "invalid-12345",
			tagKeys:    []string{"key"},
			setupMock:  func(m *mocks.MockFSx) {},
			expError:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()

			mockFSx := mocks.NewMockFSx(mockCtrl)
			tc.setupMock(mockFSx)

			c := &cloud{
				fsx: mockFSx,
			}

			err := c.UntagResource(context.TODO(), tc.resourceId, tc.tagKeys)
			if tc.expError && err == nil {
				t.Fatal("Expected error, got none")
			}
			if !tc.expError && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}
