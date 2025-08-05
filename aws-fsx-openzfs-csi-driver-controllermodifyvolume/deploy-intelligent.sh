#!/bin/bash

echo "Deploying Intelligent Tiering FSx OpenZFS..."

# Apply StorageClass
kubectl apply -f intelligent-storageclass.yaml

# Apply PVC
kubectl apply -f intelligent-claim.yaml

# Wait for PVC to be bound
echo "Waiting for PVC to be bound..."
kubectl wait --for=condition=Bound pvc/fsx-intelligent-claim --timeout=300s

# Get filesystem ID
echo "Getting filesystem ID..."
kubectl get pv -o jsonpath='{.items[?(@.spec.claimRef.name=="fsx-intelligent-claim")].spec.csi.volumeHandle}'

# Apply Pod
kubectl apply -f intelligent-pod.yaml

echo "Deployment complete!"
echo "Check status with: kubectl get pods,pvc,pv"