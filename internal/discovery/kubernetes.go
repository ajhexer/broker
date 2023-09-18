package discovery

import (
	"context"
	"fmt"
	"github.com/prometheus/common/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"os"
)

type KubernetesDiscovery struct {
	namespace     string
	labelSelector string
	hostName      string
}

func NewKubernetesDiscovery(namespace string, hostName string) *KubernetesDiscovery {

	return &KubernetesDiscovery{
		namespace: namespace,
		hostName:  hostName,
	}
}

func (k *KubernetesDiscovery) GetIPAddresses() ([]string, error, string) {
	addresses := make([]string, 0)
	var localAddress string
	config, err := rest.InClusterConfig()

	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error(os.Stderr, "Error creating Kubernetes clientSet: %v\n", err)
		return nil, err, ""
	}

	pods, err := clientSet.CoreV1().Pods(k.namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: k.labelSelector,
	})
	if err != nil {
		log.Error(os.Stderr, "Error listing pods: %v\n", err)
		return nil, err, ""
	}

	for _, pod := range pods.Items {
		if pod.Name == k.hostName {
			localAddress = pod.Status.PodIP
			continue
		}
		for _, container := range pod.Spec.Containers {
			for _, port := range container.Ports {
				if port.Name == "serf" {

					addresses = append(addresses, fmt.Sprintf("%v:%v", pod.Status.PodIP, port.ContainerPort))
				}
			}
		}
	}
	return addresses, nil, localAddress

}
