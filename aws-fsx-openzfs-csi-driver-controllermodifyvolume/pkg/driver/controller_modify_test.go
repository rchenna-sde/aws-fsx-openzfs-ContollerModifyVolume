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

package driver

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-sigs/aws-fsx-openzfs-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/aws-fsx-openzfs-csi-driver/pkg/driver/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestControllerModifyVolume(t *testing.T) {
	testCases := []struct {
		name     string
		req      *csi.ControllerModifyVolumeRequest
		expResp  *csi.ControllerModifyVolumeResponse
		expError codes.Code
	}{
		{
			name: "success - tag operation",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: "fsvol-12345",
				MutableParameters: map[string]string{
					"addTag.environment": "test",
					"addTag.team":        "dev",
				},
			},
			expResp: &csi.ControllerModifyVolumeResponse{},
		},

		{
			name: "success - regular volume modification",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: "fsvol-12345",
				MutableParameters: map[string]string{
					"StorageCapacityQuotaGiB": "100",
				},
			},
			expResp: &csi.ControllerModifyVolumeResponse{},
		},
		{
			name: "fail - missing volume ID",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: "",
			},
			expError: codes.InvalidArgument,
		},
		{
			name: "success - mixed volume and tag parameters",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: "fsvol-12345",
				MutableParameters: map[string]string{
					"StorageCapacityQuotaGiB": "1000",
					"addTag.environment":      "production",
					"addTag.team":             "platform",
				},
			},
			expResp: &csi.ControllerModifyVolumeResponse{},
		},
		{
			name: "success - volume modification with different ID",
			req: &csi.ControllerModifyVolumeRequest{
				VolumeId: "fsvol-67890",
				MutableParameters: map[string]string{
					"StorageCapacityQuotaGiB": "200",
				},
			},
			expError: codes.NotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCloud := &cloud.FakeCloudProvider{}
			awsDriver := controllerService{
				cloud:    mockCloud,
				inFlight: internal.NewInFlight(),
			}

			resp, err := awsDriver.ControllerModifyVolume(context.TODO(), tc.req)
			if tc.expError != codes.OK {
				expectError(t, err, tc.expError)
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if resp == nil {
					t.Fatal("Expected response, got nil")
				}
			}
		})
	}
}

func TestVolumeParameterModification(t *testing.T) {
	testCases := []struct {
		name       string
		parameters map[string]string
		expectPass bool
	}{
		{
			name: "storage capacity quota",
			parameters: map[string]string{
				"StorageCapacityQuotaGiB": "500",
			},
			expectPass: true,
		},
		{
			name: "data compression ZSTD",
			parameters: map[string]string{
				"DataCompressionType": "ZSTD",
			},
			expectPass: true,
		},
		{
			name: "record size 128 KiB",
			parameters: map[string]string{
				"RecordSizeKiB": "128",
			},
			expectPass: true,
		},
		{
			name: "read only true",
			parameters: map[string]string{
				"ReadOnly": "true",
			},
			expectPass: true,
		},
		{
			name: "multiple parameters with tags",
			parameters: map[string]string{
				"StorageCapacityQuotaGiB": "1000",
				"DataCompressionType":     "ZSTD",
				"addTag.env":              "prod",
			},
			expectPass: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCloud := &cloud.FakeCloudProvider{}
			awsDriver := controllerService{
				cloud:    mockCloud,
				inFlight: internal.NewInFlight(),
			}

			req := &csi.ControllerModifyVolumeRequest{
				VolumeId:          "fsvol-12345",
				MutableParameters: tc.parameters,
			}

			resp, err := awsDriver.ControllerModifyVolume(context.TODO(), req)

			if tc.expectPass {
				if err != nil {
					t.Fatalf("Expected success but got error: %v", err)
				}
				if resp == nil {
					t.Fatal("Expected response but got nil")
				}
			} else {
				if err == nil {
					t.Fatal("Expected error but got success")
				}
			}
		})
	}
}

func expectError(t *testing.T, actualErr error, expectedCode codes.Code) {
	if actualErr == nil {
		t.Fatalf("Expected error with code %v, got no error", expectedCode)
	}

	status, ok := status.FromError(actualErr)
	if !ok {
		t.Fatalf("Expected gRPC status error, got %v", actualErr)
	}

	if status.Code() != expectedCode {
		t.Fatalf("Expected error code %v, got %v. Error: %v", expectedCode, status.Code(), actualErr)
	}
}
