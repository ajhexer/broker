package discovery

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"log"
	"strings"
)

const (
	defaultK8sDiscoveryNodePortName = "broker"
	defaultK8sDiscoverySvcType      = "broker"
)

type KubernetesDiscovery struct {
	namespace             string
	matchingServiceLabels map[string]string
	nodePortName          string
}

func NewKubernetesDiscovery(namespace string, serviceLabels map[string]string, raftPortName string) *KubernetesDiscovery {
	if raftPortName == "" {
		raftPortName = defaultK8sDiscoveryNodePortName
	}
	if serviceLabels == nil || len(serviceLabels) == 0 {
		serviceLabels = make(map[string]string)
		serviceLabels["svcType"] = defaultK8sDiscoverySvcType
	}
	return &KubernetesDiscovery{
		namespace:             namespace,
		matchingServiceLabels: serviceLabels,
		nodePortName:          raftPortName,
	}
}
func (k *KubernetesDiscovery) GetIPAddresses() ([]string, error) {
	addresses := make([]string, 0)
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	services, err := clientSet.CoreV1().Services(k.namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(k.matchingServiceLabels).String(),
		Watch:         false,
	})
	if err != nil {
		log.Println(err)
		return nil, err
	}

	for _, svc := range services.Items {
		set := labels.Set(svc.Spec.Selector)
		listOptions := metav1.ListOptions{
			LabelSelector: labels.SelectorFromSet(set).String(),
		}
		pods, err := clientSet.CoreV1().Pods(svc.Namespace).List(context.Background(), listOptions)
		if err != nil {
			log.Println(err)
			continue
		}
		for _, pod := range pods.Items {
			if strings.ToLower(string(pod.Status.Phase)) == "running" {
				podIp := pod.Status.PodIP
				var raftPort v1.ContainerPort
				for _, container := range pod.Spec.Containers {
					for _, port := range container.Ports {
						if port.Name == k.nodePortName {
							raftPort = port
							break
						}
					}
				}
				if podIp != "" && raftPort.ContainerPort != 0 {
					addresses = append(addresses, fmt.Sprintf("%v:%v", podIp, raftPort.ContainerPort))
				}

			}
		}
	}

	return addresses, nil

}
