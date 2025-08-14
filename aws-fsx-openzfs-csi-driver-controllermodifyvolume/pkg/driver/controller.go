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
	"errors"
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-sigs/aws-fsx-openzfs-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/aws-fsx-openzfs-csi-driver/pkg/driver/internal"
	"github.com/kubernetes-sigs/aws-fsx-openzfs-csi-driver/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
	"os"
	"strconv"
	"strings"
)

var (
	// volumeCaps represents how volumes can be accessed.
	volumeCaps = []csi.VolumeCapability_AccessMode{
		{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
		{
			Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		},
	}

	// controllerCaps represents the capabilities of controller service
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_MODIFY_VOLUME,
	}
)

const (
	volumeContextDnsName             = "DNSName"
	volumeContextResourceType        = "ResourceType"
	volumeContextVolumePath          = "VolumePath"
	volumeParamsClientRequestToken   = "ClientRequestToken"
	volumeParamsFileSystemId         = "FileSystemId"
	volumeParamsFileSystemType       = "FileSystemType"
	volumeParamsName                 = "Name"
	volumeParamsOpenZFSConfiguration = "OpenZFSConfiguration"
	volumeParamsOriginSnapshot       = "OriginSnapshot"
	volumeParamsResourceType         = "ResourceType"
	volumeParamsSkipFinalBackup      = "SkipFinalBackup"
	volumeParamsStorageCapacity      = "StorageCapacity"
	volumeParamsTags                 = "Tags"
	volumeParamsVolumeId             = "VolumeId"
	volumeParamsVolumeType           = "VolumeType"
)

const (
	AwsFsxOpenZfsDriverTagKey  = "fsx.openzfs.csi.aws.com/cluster"
	reservedVolumeParamsPrefix = "csi.storage.k8s.io"
	deletionSuffix             = "OnDeletion"
	tagAddPrefix               = "addTag"
	tagRemovePrefix            = "removeTag"
)

// Resource Types
const (
	fsType       = "filesystem"
	volType      = "volume"
	snapshotType = "snapshot"
)

// Errors
const (
	ErrContainsDriverProviderParameter = "Contains parameter that is defined by driver: %s"
	ErrIncorrectlyFormatted            = "%s is incorrectly formatted: %s"
	ErrResourceTypeNotProvided         = "ResourceType is not provided"
	ErrResourceTypeNotSupported        = "ResourceType is not supported: %s"
)

// controllerService represents the controller service of CSI driver
type controllerService struct {
	csi.UnimplementedControllerServer
	cloud         cloud.Cloud
	inFlight      *internal.InFlight
	driverOptions *DriverOptions
}

// newControllerService creates a new controller service
// it panics if failed to create the service
func newControllerService(driverOptions *DriverOptions) controllerService {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		klog.V(5).InfoS("[Debug] Retrieving region from metadata service")
		metadata, err := cloud.NewMetadataService(cloud.DefaultIMDSClient, cloud.DefaultKubernetesAPIClient, region)
		if err != nil {
			klog.ErrorS(err, "Could not determine region from any metadata service. The region can be manually supplied via the AWS_REGION environment variable.")
			panic(err)
		}
		region = metadata.GetRegion()
	}

	klog.InfoS("regionFromSession Controller service", "region", region)

	cloudSrv, err := cloud.NewCloud(region)
	if err != nil {
		panic(err)
	}

	return controllerService{
		cloud:         cloudSrv,
		inFlight:      internal.NewInFlight(),
		driverOptions: driverOptions,
	}
}

func (d *controllerService) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	klog.V(4).InfoS("CreateVolume: called with", "args", *req)

	volName := req.GetName()
	if len(volName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume name not provided")
	}

	// check if a request is already in-flight
	if ok := d.inFlight.Insert(volName); !ok {
		msg := fmt.Sprintf("CreateVolume: "+internal.VolumeOperationAlreadyExistsErrorMsg, volName)
		return nil, status.Error(codes.Aborted, msg)
	}
	defer d.inFlight.Delete(volName)

	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not provided")
	}
	if !isValidVolumeCapabilities(volCaps) {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not supported")
	}

	var storageCapacity int32
	if req.GetCapacityRange() != nil {
		storageCapacity = util.BytesToGiB(req.GetCapacityRange().GetRequiredBytes())
	}

	volumeParams := req.GetParameters()
	if volumeParams == nil {
		volumeParams = make(map[string]string)
	}

	deleteReservedParameters(volumeParams)

	resourceType := volumeParams[volumeParamsResourceType]
	if resourceType != fsType && resourceType != volType {
		if resourceType == "" {
			return nil, status.Error(codes.InvalidArgument, ErrResourceTypeNotProvided)
		}
		return nil, status.Errorf(codes.InvalidArgument, ErrResourceTypeNotSupported, resourceType)
	}
	delete(volumeParams, volumeParamsResourceType)

	err := containsDriverProvidedParameters(volumeParams)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	volumeParams[volumeParamsClientRequestToken] = strconv.Quote(volName)

	err = appendDeleteTags(volumeParams, resourceType)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, ErrIncorrectlyFormatted, "Delete Parameters", err)
	}

	err = appendCustomTags(volumeParams, resourceType)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if resourceType == fsType {
		if req.GetVolumeContentSource() != nil {
			return nil, status.Error(codes.Unimplemented, "Cannot create new file system from a snapshot. To create a new volume from a snapshot, set ResourceType to volume.")
		}

		volumeParams[volumeParamsFileSystemType] = strconv.Quote("OPENZFS")
		volumeParams[volumeParamsStorageCapacity] = strconv.Itoa(
			int(storageCapacity),
		)

		storageType := volumeParams["StorageType"]
		if storageType == `"INTELLIGENT_TIERING"` {
			if storageCapacity != 1 {
				return nil, status.Error(codes.InvalidArgument, "storageType INTELLIGENT_TIERING expects storage capacity to be 1Gi")
			}
			delete(volumeParams, volumeParamsStorageCapacity)
		}
		err = cloud.CollapseCreateFileSystemParameters(volumeParams)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, ErrIncorrectlyFormatted, "OpenZFSConfiguration", err)
		}

		fs, err := d.cloud.CreateFileSystem(ctx, volumeParams)
		if err != nil {
			klog.V(4).InfoS("CreateFileSystem", "error", err.Error())
			switch {
			case errors.Is(err, cloud.ErrInvalidInput):
				return nil, status.Error(codes.InvalidArgument, err.Error())
			case errors.Is(err, cloud.ErrAlreadyExists):
				return nil, status.Error(codes.AlreadyExists, err.Error())
			default:
				return nil, status.Errorf(codes.Internal, "Could not create volume %q: %v", volName, err)
			}
		}

		err = d.cloud.WaitForFileSystemAvailable(ctx, fs.FileSystemId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Filesystem is not ready: %v", err)
		}

		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				CapacityBytes: util.GiBToBytes(fs.StorageCapacity),
				VolumeId:      fs.FileSystemId,
				VolumeContext: map[string]string{
					volumeContextDnsName:      fs.DnsName,
					volumeContextResourceType: resourceType,
				},
			},
		}, nil
	}
	if resourceType == volType {
		var volumeContentSource *csi.VolumeContentSource

		if storageCapacity != 1 {
			return nil, status.Error(codes.InvalidArgument, "resourceType Volume expects storage capacity to be 1Gi")
		}

		volumeSource := req.GetVolumeContentSource()
		if volumeSource != nil {
			if _, ok := volumeSource.GetType().(*csi.VolumeContentSource_Snapshot); !ok {
				return nil, status.Error(codes.Unimplemented, "Unsupported volumeContentSource type")
			}

			sourceSnapshot := volumeSource.GetSnapshot()
			if sourceSnapshot == nil {
				return nil, status.Error(codes.InvalidArgument, "Error retrieving snapshot from the volumeContentSource")
			}
			snapshotId := sourceSnapshot.GetSnapshotId()

			err = d.appendSnapshotARN(ctx, volumeParams, snapshotId)
			if err != nil {
				if err == cloud.ErrNotFound {
					return nil, status.Errorf(codes.NotFound, "Snapshot not found with ID %q", snapshotId)
				}
				return nil, status.Errorf(codes.Internal, "Could not get snapshot with ID %q: %v", snapshotId, err)
			}

			volumeContentSource = &csi.VolumeContentSource{
				Type: &csi.VolumeContentSource_Snapshot{
					Snapshot: &csi.VolumeContentSource_SnapshotSource{
						SnapshotId: snapshotId,
					},
				},
			}
		}

		volumeParams[volumeParamsName] = strconv.Quote(volName)
		volumeParams[volumeParamsVolumeType] = strconv.Quote("OPENZFS")
		err = cloud.CollapseCreateVolumeParameters(volumeParams)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, ErrIncorrectlyFormatted, "OpenZFSConfiguration", err)
		}

		v, err := d.cloud.CreateVolume(ctx, volumeParams)
		if err != nil {
			klog.V(4).InfoS("CreateVolume", "error", err.Error())
			switch {
			case errors.Is(err, cloud.ErrInvalidInput):
				return nil, status.Error(codes.InvalidArgument, err.Error())
			case errors.Is(err, cloud.ErrAlreadyExists):
				return nil, status.Error(codes.AlreadyExists, err.Error())
			default:
				return nil, status.Errorf(codes.Internal, "Could not create volume %q: %v", volName, err)
			}
		}

		err = d.cloud.WaitForVolumeAvailable(ctx, v.VolumeId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Volume is not ready: %v", err)
		}

		fileSystem, err := d.cloud.DescribeFileSystem(ctx, v.FileSystemId)
		if err != nil {
			return nil, err
		}

		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				CapacityBytes: util.GiBToBytes(storageCapacity),
				VolumeId:      v.VolumeId,
				VolumeContext: map[string]string{
					volumeContextDnsName:      fileSystem.DnsName,
					volumeContextResourceType: resourceType,
					volumeContextVolumePath:   v.VolumePath,
				},
				ContentSource: volumeContentSource,
			},
		}, nil
	}
	return nil, status.Errorf(codes.InvalidArgument, "Type %s not supported", volumeContextResourceType)
}

func (d *controllerService) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	var err error

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumeId is empty")
	}

	// check if a request is already in-flight
	if ok := d.inFlight.Insert(volumeID); !ok {
		msg := fmt.Sprintf("DeleteVolume: "+internal.VolumeOperationAlreadyExistsErrorMsg, volumeID)
		return nil, status.Error(codes.Aborted, msg)
	}
	defer d.inFlight.Delete(volumeID)

	deleteParams, err := d.cloud.GetDeleteParameters(ctx, volumeID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	splitVolumeId := strings.SplitN(volumeID, "-", 2)
	if splitVolumeId[0] == cloud.FilesystemPrefix {
		deleteParams[volumeParamsFileSystemId] = strconv.Quote(volumeID)
		err = cloud.CollapseDeleteFileSystemParameters(deleteParams)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		err = d.cloud.DeleteFileSystem(ctx, deleteParams)
	}
	if splitVolumeId[0] == cloud.VolumePrefix {
		deleteParams[volumeParamsVolumeId] = strconv.Quote(volumeID)
		err = cloud.CollapseDeleteVolumeParameters(deleteParams)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		err = d.cloud.DeleteVolume(ctx, deleteParams)
	}

	if err != nil {
		if err == cloud.ErrNotFound {
			klog.V(4).InfoS("DeleteVolume: volume not found, returning with success", "volumeId", volumeID)
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "Could not delete volume ID %q: %v", volumeID, err)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (d *controllerService) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	klog.V(4).InfoS("ValidateVolumeCapabilities: called with", "args", *req)
	var err error

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not provided")
	}

	splitVolumeId := strings.SplitN(volumeID, "-", 2)
	if splitVolumeId[0] == cloud.FilesystemPrefix {
		_, err = d.cloud.DescribeFileSystem(ctx, volumeID)
	} else if splitVolumeId[0] == cloud.VolumePrefix {
		_, err = d.cloud.DescribeVolume(ctx, volumeID)
	} else {
		err = cloud.ErrNotFound
	}

	if err != nil {
		if err == cloud.ErrNotFound {
			return nil, status.Errorf(codes.NotFound, "Volume not found with ID %q", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "Could not get volume with ID %q: %v", volumeID, err)
	}

	confirmed := isValidVolumeCapabilities(volCaps)
	if confirmed {
		return &csi.ValidateVolumeCapabilitiesResponse{
			Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
				// TODO if volume context is provided, should validate it too
				//  VolumeContext:      req.GetVolumeContext(),
				VolumeCapabilities: volCaps,
				// TODO if parameters are provided, should validate them too
				//  Parameters:      req.GetParameters(),
			},
		}, nil
	} else {
		return &csi.ValidateVolumeCapabilitiesResponse{}, nil
	}
}

// isValidVolumeCapabilities Validates the accessMode support for the volume capabilities
func isValidVolumeCapabilities(volCaps []*csi.VolumeCapability) bool {
	hasSupport := func(cap *csi.VolumeCapability) bool {
		for _, c := range volumeCaps {
			if c.GetMode() == cap.AccessMode.GetMode() {
				return true
			}
		}
		return false
	}

	foundAll := true
	for _, c := range volCaps {
		if !hasSupport(c) {
			foundAll = false
		}
	}
	return foundAll
}

func (d *controllerService) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(4).InfoS("ControllerGetCapabilities: called with", "args", *req)
	var caps []*csi.ControllerServiceCapability
	for _, cap := range controllerCaps {
		c := &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		}
		caps = append(caps, c)
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (d *controllerService) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	klog.V(4).InfoS("CreateSnapshot: called with", "args", *req)

	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot name not provided")
	}

	if len(req.GetSourceVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot volume source ID not provided")
	}

	// check if a request is already in-flight
	if ok := d.inFlight.Insert(req.GetName()); !ok {
		msg := fmt.Sprintf("CreateSnapshot: "+internal.SnapshotOperationAlreadyExistsErrorMsg, req.GetName())
		return nil, status.Error(codes.Aborted, msg)
	}
	defer d.inFlight.Delete(req.GetName())

	snapshotParams := req.GetParameters()
	if snapshotParams == nil {
		snapshotParams = make(map[string]string)
	}

	deleteReservedParameters(snapshotParams)

	err := containsDriverProvidedParameters(snapshotParams)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	err = appendCustomTags(snapshotParams, snapshotType)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	volumeId, err := d.cloud.GetVolumeId(ctx, req.GetSourceVolumeId())
	if err != nil {
		return nil, status.Error(codes.Internal, "Custom tags incorrectly added")
	}

	snapshotParams[volumeParamsClientRequestToken] = strconv.Quote(req.GetName())
	snapshotParams[volumeParamsVolumeId] = strconv.Quote(volumeId)
	snapshotParams[volumeParamsName] = strconv.Quote(req.GetName())

	snapshot, err := d.cloud.CreateSnapshot(ctx, snapshotParams)
	if err != nil {
		switch {
		case errors.Is(err, cloud.ErrInvalidInput):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, cloud.ErrAlreadyExists):
			return nil, status.Error(codes.AlreadyExists, err.Error())
		default:
			return nil, status.Errorf(codes.Internal, "CreateSnapshot: Failed to create snapshot %q with error %v", req.GetName(), err)
		}
	}

	err = d.cloud.WaitForSnapshotAvailable(ctx, snapshot.SnapshotID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Snapshot %s is not ready: %v", req.GetName(), err)
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapshot.SnapshotID,
			SourceVolumeId: snapshot.SourceVolumeID,
			CreationTime:   timestamppb.New(snapshot.CreationTime),
			ReadyToUse:     true,
		},
	}, nil
}

func (d *controllerService) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	klog.V(4).InfoS("DeleteSnapshot: called with", "args", *req)
	deleteParams := make(map[string]string)
	snapshotId := req.GetSnapshotId()

	if len(snapshotId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot ID not provided")
	}

	// check if a request is already in-flight
	if ok := d.inFlight.Insert(snapshotId); !ok {
		msg := fmt.Sprintf("DeleteSnapshot: "+internal.SnapshotOperationAlreadyExistsErrorMsg, snapshotId)
		return nil, status.Error(codes.Aborted, msg)
	}
	defer d.inFlight.Delete(snapshotId)

	deleteParams["SnapshotId"] = strconv.Quote(snapshotId)

	if err := d.cloud.DeleteSnapshot(ctx, deleteParams); err != nil {
		if strings.Contains(err.Error(), "Unable to find snapshot") {
			klog.V(4).InfoS("DeleteSnapshot: Snapshot not found, returning with success", "snapshotId", snapshotId)
			return &csi.DeleteSnapshotResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "DeleteSnapshot: Could not delete snapshot %s, received error %v", snapshotId, err)
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

func (d *controllerService) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	klog.V(4).InfoS("ControllerExpandVolume: called with", "args", *req)

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	// check if a request is already in-flight
	if ok := d.inFlight.Insert(volumeID); !ok {
		msg := fmt.Sprintf("ControllerExpandVolume: "+internal.VolumeOperationAlreadyExistsErrorMsg, volumeID)
		return nil, status.Error(codes.Aborted, msg)
	}
	defer d.inFlight.Delete(volumeID)

	capRange := req.GetCapacityRange()
	if capRange == nil {
		return nil, status.Error(codes.InvalidArgument, "Capacity range not provided")
	}
	if capRange.GetLimitBytes() > 0 && capRange.GetRequiredBytes() > capRange.GetLimitBytes() {
		return nil, status.Errorf(codes.OutOfRange, "Requested storage capacity of %d bytes exceeds capacity limit of %d bytes.", capRange.GetRequiredBytes(), capRange.GetLimitBytes())
	}

	newCapacity := util.BytesToGiB(capRange.GetRequiredBytes())

	splitVolumeId := strings.SplitN(volumeID, "-", 2)
	if splitVolumeId[0] == cloud.FilesystemPrefix {
		fs, err := d.cloud.DescribeFileSystem(ctx, volumeID)
		if err != nil {
			if err == cloud.ErrNotFound {
				return nil, status.Errorf(codes.NotFound, "Filesystem not found with ID %q", volumeID)
			}
			return nil, status.Errorf(codes.Internal, "Could not get filesystem with ID %q: %v", volumeID, err)
		}

		if newCapacity <= (fs.StorageCapacity) {
			// Current capacity is sufficient to satisfy the request
			klog.V(4).InfoS("ControllerExpandVolume: current filesystem capacity matches or exceeds requested storage capacity, returning with success", "currentStorageCapacityGiB", fs.StorageCapacity, "requestedStorageCapacityGiB", newCapacity)
			return &csi.ControllerExpandVolumeResponse{
				CapacityBytes:         util.GiBToBytes(fs.StorageCapacity),
				NodeExpansionRequired: false,
			}, nil
		}

		finalCapacity, err := d.cloud.ResizeFileSystem(ctx, volumeID, int32(newCapacity))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "resize failed: %v", err)
		}

		err = d.cloud.WaitForFileSystemResize(ctx, volumeID, *finalCapacity)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "filesystem is not resized: %v", err)
		}

		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         util.GiBToBytes(*finalCapacity),
			NodeExpansionRequired: false,
		}, nil
	}

	if splitVolumeId[0] == cloud.VolumePrefix {
		return nil, status.Error(codes.Unimplemented, "Storage of ResourceType Volume can not be scaled")
	}
	return nil, status.Errorf(codes.NotFound, "Volume not found with ID %q", volumeID)
}

func (d *controllerService) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) mustEmbedUnimplementedControllerServer() {}
func (d *controllerService) ControllerModifyVolume(ctx context.Context, req *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	klog.V(4).InfoS("ControllerModifyVolume: called with", "args", *req)

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	if ok := d.inFlight.Insert(volumeID); !ok {
		msg := fmt.Sprintf("ControllerModifyVolume: "+internal.VolumeOperationAlreadyExistsErrorMsg, volumeID)
		return nil, status.Error(codes.Aborted, msg)
	}
	defer d.inFlight.Delete(volumeID)

	mutableParameters := req.GetMutableParameters()
	if mutableParameters == nil {
		mutableParameters = make(map[string]string)
	}

	deleteReservedParameters(mutableParameters)

	// Validate parameter syntax first
	err := d.validateParameterSyntax(mutableParameters)
	if err != nil {
		return nil, err
	}

	// Categorize parameters after validation
	volumeParams, addTags, removeTags := categorizeParameters(mutableParameters)

	// Validate resource exists for tag operations
	if len(addTags) > 0 || len(removeTags) > 0 {
		_, err = d.getResourceARN(ctx, volumeID)
		if err != nil {
			return nil, err
		}
	}

	// Execute all operations - fail fast on any error
	if len(volumeParams) > 0 {
		_, err = d.cloud.ModifyVolume(ctx, volumeID, volumeParams)
		if err != nil {
			if err == cloud.ErrNotFound {
				return nil, status.Errorf(codes.NotFound, "Volume not found with ID %q", volumeID)
			}
			return nil, status.Errorf(codes.Internal, "Could not modify volume %q: %v", volumeID, err)
		}
	}

	if len(addTags) > 0 {
		err = d.cloud.TagResource(ctx, volumeID, addTags)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Could not add tags to volume %q: %v", volumeID, err)
		}
	}

	if len(removeTags) > 0 {
		err = d.cloud.UntagResource(ctx, volumeID, removeTags)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Could not remove tags from volume %q: %v", volumeID, err)
		}
	}

	return &csi.ControllerModifyVolumeResponse{}, nil
}

// getResourceARN retrieves the resource ARN for tag operations
func (d *controllerService) getResourceARN(ctx context.Context, volumeID string) (string, error) {
	// Validate volume exists first
	splitVolumeId := strings.SplitN(volumeID, "-", 2)
	if splitVolumeId[0] == cloud.FilesystemPrefix {
		_, err := d.cloud.DescribeFileSystem(ctx, volumeID)
		if err != nil {
			if err == cloud.ErrNotFound {
				return "", status.Errorf(codes.NotFound, "Filesystem not found with ID %q", volumeID)
			}
			return "", status.Errorf(codes.Internal, "Could not get filesystem %q: %v", volumeID, err)
		}
	} else if splitVolumeId[0] == cloud.VolumePrefix {
		_, err := d.cloud.DescribeVolume(ctx, volumeID)
		if err != nil {
			if err == cloud.ErrNotFound {
				return "", status.Errorf(codes.NotFound, "Volume not found with ID %q", volumeID)
			}
			return "", status.Errorf(codes.Internal, "Could not get volume %q: %v", volumeID, err)
		}
	} else {
		return "", status.Errorf(codes.InvalidArgument, "Invalid volume ID format: %q", volumeID)
	}

	// Return volumeID - the cloud layer's TagResource/UntagResource methods handle ARN resolution
	return volumeID, nil
}

// validateParameterSyntax validates parameter syntax and format
func (d *controllerService) validateParameterSyntax(parameters map[string]string) error {
	// Separate parameters for validation
	volumeParams := make(map[string]string)
	addTags := make(map[string]string)
	var removeTags []string

	for k, v := range parameters {
		if strings.HasPrefix(k, tagAddPrefix+".") {
			tagKey := strings.TrimPrefix(k, tagAddPrefix+".")
			addTags[tagKey] = v
		} else if strings.HasPrefix(k, tagRemovePrefix+".") {
			tagKey := strings.TrimPrefix(k, tagRemovePrefix+".")
			removeTags = append(removeTags, tagKey)
		} else {
			volumeParams[k] = v
		}
	}

	// Validate volume parameters syntax
	if len(volumeParams) > 0 {
		err := d.validateVolumeParameterSyntax(volumeParams)
		if err != nil {
			return err
		}
	}

	// Validate tag parameters syntax
	if len(addTags) > 0 {
		err := d.validateTagParameterSyntax(addTags)
		if err != nil {
			return err
		}
	}

	// Validate remove tags syntax
	if len(removeTags) > 0 {
		err := d.validateRemoveTagSyntax(removeTags)
		if err != nil {
			return err
		}
	}

	return nil
}

// validateVolumeParameterSyntax validates volume parameter syntax only
func (d *controllerService) validateVolumeParameterSyntax(params map[string]string) error {
	validParams := map[string]bool{
		"StorageCapacityQuotaGiB": true,
		"DataCompressionType":     true,
		"RecordSizeKiB":           true,
		"ReadOnly":                true,
		"UserAndGroupQuotas":      true,
	}

	for key, value := range params {
		if !validParams[key] {
			return status.Errorf(codes.InvalidArgument, "Invalid volume parameter: %s", key)
		}

		// Validate specific parameter values
		switch key {
		case "StorageCapacityQuotaGiB":
			if capacity, err := strconv.Atoi(value); err != nil || capacity < 1 {
				return status.Errorf(codes.InvalidArgument, "Invalid StorageCapacityQuotaGiB: %s", value)
			}
		case "DataCompressionType":
			if value != "NONE" && value != "ZSTD" && value != "LZ4" {
				return status.Errorf(codes.InvalidArgument, "Invalid DataCompressionType: %s", value)
			}
		case "RecordSizeKiB":
			if size, err := strconv.Atoi(value); err != nil || size < 1 || size > 1048576 {
				return status.Errorf(codes.InvalidArgument, "Invalid RecordSizeKiB: %s", value)
			}
		case "ReadOnly":
			if value != "true" && value != "false" {
				return status.Errorf(codes.InvalidArgument, "Invalid ReadOnly value: %s", value)
			}
		}
	}
	return nil
}

// validateTagParameterSyntax validates tag parameter syntax only
func (d *controllerService) validateTagParameterSyntax(tags map[string]string) error {
	for key, value := range tags {
		if len(key) == 0 || len(key) > 128 {
			return status.Errorf(codes.InvalidArgument, "Invalid tag key length: %s", key)
		}
		if len(value) > 256 {
			return status.Errorf(codes.InvalidArgument, "Invalid tag value length for key %s", key)
		}
		if strings.HasPrefix(key, "aws:") {
			return status.Errorf(codes.InvalidArgument, "Cannot modify AWS reserved tag: %s", key)
		}
	}
	return nil
}

// validateRemoveTagSyntax validates remove tag syntax only
func (d *controllerService) validateRemoveTagSyntax(tags []string) error {
	for _, key := range tags {
		if len(key) == 0 || len(key) > 128 {
			return status.Errorf(codes.InvalidArgument, "Invalid tag key length: %s", key)
		}
		if strings.HasPrefix(key, "aws:") {
			return status.Errorf(codes.InvalidArgument, "Cannot remove AWS reserved tag: %s", key)
		}
	}
	return nil
}

// categorizeParameters separates parameters into volume params, add tags, and remove tags
func categorizeParameters(parameters map[string]string) (map[string]string, map[string]string, []string) {
	volumeParams := make(map[string]string)
	addTags := make(map[string]string)
	var removeTags []string

	for k, v := range parameters {
		if strings.HasPrefix(k, tagAddPrefix+".") {
			tagKey := strings.TrimPrefix(k, tagAddPrefix+".")
			addTags[tagKey] = v
		} else if strings.HasPrefix(k, tagRemovePrefix+".") {
			tagKey := strings.TrimPrefix(k, tagRemovePrefix+".")
			removeTags = append(removeTags, tagKey)
		} else {
			volumeParams[k] = v
		}
	}

	return volumeParams, addTags, removeTags
}

// deleteReservedParameters removes reserved parameters that are populated in request parameters
// Reserved parameters are deleted directly on the parameters map
func deleteReservedParameters(parameters map[string]string) {
	for key, _ := range parameters {
		if strings.HasPrefix(key, reservedVolumeParamsPrefix) {
			delete(parameters, key)
		}
	}
}

// containsDriverProvidedParameters checks if parameters contains a JSON field that will be provided by CSI driver.
// Returns error if it is defined. Returns nil if it doesn't contain a field.
func containsDriverProvidedParameters(parameters map[string]string) error {
	driverDefinedParameters := []string{
		volumeParamsClientRequestToken,
		volumeParamsFileSystemType,
		volumeParamsName,
		volumeParamsOpenZFSConfiguration,
		volumeParamsStorageCapacity,
		volumeParamsVolumeId,
		volumeParamsVolumeType,
	}

	for _, parameter := range driverDefinedParameters {
		_, ok := parameters[parameter]
		if ok {
			return errors.New(fmt.Sprintf(ErrContainsDriverProviderParameter, parameter))
		}
	}

	if strings.Contains(parameters[volumeParamsOriginSnapshot], "SnapshotARN") {
		return errors.New(fmt.Sprintf(ErrContainsDriverProviderParameter, "SnapshotARN"))
	}

	return nil
}

// appendCustomTags appends custom CSI driver tags to resources created.
// Added tags are directly combined to the Tags field in parameters
// Errors if the provided parameters is not an expected json
func appendCustomTags(parameters map[string]string, resourceType string) error {
	//Create object containing existing tags
	var existingTags []map[string]string
	err := util.ConvertJsonStringToObject(parameters[volumeParamsTags], &existingTags)
	if err != nil {
		return err
	}

	existingTags = append(existingTags, map[string]string{"Key": AwsFsxOpenZfsDriverTagKey, "Value": "true"})

	//Put the combined Tags json on parameters
	combinedJsonString, err := util.ConvertObjectToJsonString(existingTags)
	if err != nil {
		return err
	}
	parameters[volumeParamsTags] = combinedJsonString

	return nil
}

// appendDeleteTags converts all delete parameters provided to tag format and appends it to the Tags field.
// Delete parameters should contain the suffix "OnDeletion"
// Converted parameters are directly deleted off of parameters, and the combined Tags field is added
// Validates delete parameters provided in accordance to the FSx API
// Also validates the required SkipFinalBackup parameter is includes for ResourceType FileSystem
// Errors if the provided parameters is not an expected json or is invalid
func appendDeleteTags(parameters map[string]string, resourceType string) error {
	//Store delete parameters for validation
	deleteParameters := make(map[string]string)

	//Create object containing existing tags
	var existingTags []map[string]string
	err := util.ConvertJsonStringToObject(parameters[volumeParamsTags], &existingTags)
	if err != nil {
		return err
	}

	//Convert deletion parameters to a tag, append it to the existingTags, and delete the deletion parameter
	for key, value := range parameters {
		if strings.HasSuffix(key, deletionSuffix) {
			deleteKey := strings.TrimSuffix(key, deletionSuffix)
			deleteParameters[deleteKey] = value

			if strings.ContainsAny(value, "[],\"") {
				value = util.EncodeDeletionTag(value)
			}
			existingTags = append(existingTags, map[string]string{"Key": key, "Value": value})

			delete(parameters, key)
		}
	}

	//Validate deleteParameters are compatible with their respective objects before creating the resource
	if resourceType == fsType {
		//Error if user doesn't provide CSI driver required SkipFinalBackup field
		if _, ok := deleteParameters[volumeParamsSkipFinalBackup]; !ok {
			return errors.New(fmt.Sprintf(ErrIncorrectlyFormatted, volumeParamsSkipFinalBackup, "field is required"))
		}

		deleteParameters[volumeParamsFileSystemId] = strconv.Quote("fs-1234567890abc")
		err = cloud.CollapseDeleteFileSystemParameters(deleteParameters)
		if err != nil {
			return err
		}
		err = cloud.ValidateDeleteFileSystemParameters(deleteParameters)
		if err != nil {
			return err
		}
	}
	if resourceType == volType {
		deleteParameters[volumeParamsVolumeId] = strconv.Quote("fsvol-1234567890abcdefghijm")
		err = cloud.CollapseDeleteVolumeParameters(deleteParameters)
		if err != nil {
			return err
		}
		err = cloud.ValidateDeleteVolumeParameters(deleteParameters)
		if err != nil {
			return err
		}
	}

	//Put the combined Tags json on parameters
	combinedJsonString, err := util.ConvertObjectToJsonString(existingTags)
	if err != nil {
		return err
	}
	parameters[volumeParamsTags] = combinedJsonString

	return nil
}

// appendSnapshotARN appends the snapshot arn to the OriginSnapshot parameter provided
// Directly replaces the parameters field with the new json string
func (d *controllerService) appendSnapshotARN(ctx context.Context, parameters map[string]string, snapshotId string) error {
	existingOriginSnapshot := make(map[string]string)
	err := util.ConvertJsonStringToObject(parameters[volumeParamsOriginSnapshot], &existingOriginSnapshot)
	if err != nil {
		return err
	}

	snapshot, err := d.cloud.DescribeSnapshot(ctx, snapshotId)
	if err != nil {
		return err
	}

	existingOriginSnapshot["SnapshotARN"] = snapshot.ResourceARN
	originSnapshotJsonString, err := util.ConvertObjectToJsonString(existingOriginSnapshot)
	if err != nil {
		return err
	}

	parameters[volumeParamsOriginSnapshot] = originSnapshotJsonString

	return nil
}
