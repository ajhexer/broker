package main

import (
	"context"
	"fmt"
	"github.com/prometheus/common/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"os"
)

func main() {

	addresses := make([]string, 0)

	config, err := rest.InClusterConfig()

	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error(os.Stderr, "Error creating Kubernetes clientSet: %v\n", err)

	}

	pods, err := clientSet.CoreV1().Pods("default").List(context.TODO(), metav1.ListOptions{
		LabelSelector: "app=broker",
	})
	if err != nil {
		log.Error(os.Stderr, "Error listing pods: %v\n", err)

	}

	for _, pod := range pods.Items {
		for _, container := range pod.Spec.Containers {
			for _, port := range container.Ports {
				if port.Name == "raft" {

					addresses = append(addresses, fmt.Sprintf("%v:%v", pod.Status.PodIP, port.ContainerPort))
				}
			}
		}
	}
	for _, url := range addresses {
		fmt.Println(url)
	}

}
