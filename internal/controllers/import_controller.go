package controllers

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"sigs.k8s.io/cluster-api/controllers/external"
	"sigs.k8s.io/cluster-api/util/predicates"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	gvkRancherCluster = schema.GroupVersionKind{Group: "provisioning.cattle.io", Version: "v1", Kind: "Cluster"}
)

type CAPIImportReconciler struct {
	client.Client
	recorder         record.EventRecorder
	WatchFilterValue string
	Scheme           *runtime.Scheme

	controller      controller.Controller
	externalTracker external.ObjectTracker
}

// +kubebuilder:rbac:groups=core,resources=events,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;delete;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters;clusters/status,verbs=get;list;watch
// +kubebuilder:rbac:groups=provisioning.cattle.io,resources=clusters;clusters/status,verbs=get;list;watch;create;update;delete;patch
func (r *CAPIImportReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, reterr error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling CAPI cluster")

	capiCluster := &clusterv1.Cluster{}
	if err := r.Client.Get(ctx, req.NamespacedName, capiCluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// fethc the rancher clusters
	rancherCluster, err := getOwnerRancherCluster(ctx, r.Client, capiCluster.ObjectMeta)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{Requeue: true}, err
		}
	}

	if capiCluster.ObjectMeta.DeletionTimestamp.IsZero() {
		//TODO: handle delete
	}

	return r.reconcileNormal(ctx, capiCluster, rancherCluster)
}

func (r *CAPIImportReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	log := log.FromContext(ctx)

	//TODO: we want the control plane initialized but removing for the time being
	//capiPredicates := predicates.All(log, predicates.ClusterControlPlaneInitialized(log), predicates.ResourceHasFilterLabel(log, r.WatchFilterValue))
	capiPredicates := predicates.All(log, predicates.ResourceHasFilterLabel(log, r.WatchFilterValue))

	c, err := ctrl.NewControllerManagedBy(mgr).For(&clusterv1.Cluster{}).WithEventFilter(capiPredicates).Build(r)
	if err != nil {
		return fmt.Errorf("creating new controller: %w", err)
	}

	// err = c.Watch(
	// 	&source.Kind{Type: &rancherv1.Cluster{}},
	// 	handler.EnqueueRequestsFromMapFunc(r.rancherClusterToCapiCluster),
	// )
	// if err != nil {
	// 	return fmt.Errorf("adding watch for Rancher cluster: %w", err)
	// }

	r.recorder = mgr.GetEventRecorderFor("rancher-turtles")
	r.controller = c
	r.externalTracker = external.ObjectTracker{
		Controller: c,
	}

	gvk := schema.GroupVersionKind{Group: "provisioning.cattle.io", Version: "v1", Kind: "Cluster"}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)

	err = c.Watch(
		&source.Kind{Type: u},
		&handler.EnqueueRequestForOwner{OwnerType: &clusterv1.Cluster{}},
	)
	if err != nil {
		return fmt.Errorf("adding watch for Rancher cluster: %w", err)
	}

	return nil
}

func (r *CAPIImportReconciler) reconcileNormal(ctx context.Context, capiCluster *clusterv1.Cluster, rancherCluster *unstructured.Unstructured) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling CAPI Cluster")

	if rancherCluster == nil {
		if err := r.createRancherCluster(ctx, capiCluster); err != nil {
			log.Error(err, "failed creating rancher cluster")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	return ctrl.Result{}, nil
}

func (r *CAPIImportReconciler) createRancherCluster(ctx context.Context, capiCluster *clusterv1.Cluster) error {
	//TODO: we would not use unstructured in the future, instead import the API definitions from Rancher
	rancherCluster := &unstructured.Unstructured{}
	rancherCluster.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":      capiCluster.Name,
			"namespace": capiCluster.Namespace,
		},
		"spec": map[string]interface{}{},
	})
	rancherCluster.SetGroupVersionKind(gvkRancherCluster)

	if err := r.Client.Create(ctx, rancherCluster); err != nil {
		return fmt.Errorf("creating rancher cluster: %w", err)
	}

	return nil
}

func (r *CAPIImportReconciler) rancherClusterToCapiCluster(o client.Object) []ctrl.Request {

	//c, ok := o.(*clusterv1.Cluster)
	//if !ok {
	//	r.Log.Error(nil, fmt.Sprintf("Expected a Cluster but got a %T", o))
	//	return nil
	//}
	//
	//controlPlaneRef := c.Spec.ControlPlaneRef
	//if controlPlaneRef != nil && controlPlaneRef.Kind == "RKE2ControlPlane" {
	//	return []ctrl.Request{{NamespacedName: client.ObjectKey{Namespace: controlPlaneRef.Namespace, Name: controlPlaneRef.Name}}}
	//}

	return nil
}

func getOwnerRancherCluster(ctx context.Context, c client.Client, obj metav1.ObjectMeta) (*unstructured.Unstructured, error) {
	for _, ref := range obj.OwnerReferences {
		if ref.Kind != "Cluster" {
			continue
		}
		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if gv.Group == "provisioning.cattle.io" {
			return getRancherClusterByName(ctx, c, obj.Namespace, ref.Name)
		}
	}
	return nil, nil
}

func getRancherClusterByName(ctx context.Context, c client.Client, namespace, name string) (*unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{}
	key := client.ObjectKey{Name: name, Namespace: namespace}
	if err := c.Get(ctx, key, u); err != nil {
		return nil, err
	}
	return u, nil
}
