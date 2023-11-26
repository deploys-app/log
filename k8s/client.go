package k8s

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Client struct {
	client    *kubernetes.Clientset
	namespace string
}

func NewClient(namespace string) (*Client, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &Client{
		client:    client,
		namespace: namespace,
	}, nil
}

func NewLocalClient(namespace string) (*Client, error) {
	config := &rest.Config{
		Host:    "localhost:8001",
		APIPath: "/",
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &Client{
		client:    client,
		namespace: namespace,
	}, nil
}
