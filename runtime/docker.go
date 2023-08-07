package runtime

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/tork/task"
)

type DockerRuntime struct {
	client *client.Client
	tasks  map[string]string
	mu     sync.RWMutex
}

func NewDockerRuntime() (*DockerRuntime, error) {
	dc, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	return &DockerRuntime{
		client: dc,
		tasks:  make(map[string]string),
		mu:     sync.RWMutex{},
	}, nil
}

func (d *DockerRuntime) Run(ctx context.Context, t *task.Task) (string, error) {
	reader, err := d.client.ImagePull(
		ctx, t.Image, types.ImagePullOptions{})
	if err != nil {
		log.Error().Err(err).Msgf("Error pulling image %s: %v\n", t.Image, err)
		return "", err
	}
	_, err = io.Copy(os.Stdout, reader)
	if err != nil {
		return "", err
	}

	rp := container.RestartPolicy{
		Name: t.RestartPolicy,
	}

	r := container.Resources{
		Memory: t.Memory,
	}

	env := []string{}
	for name, value := range t.Env {
		env = append(env, fmt.Sprintf("%s=%s", name, value))
	}

	cc := container.Config{
		Image: t.Image,
		Env:   env,
		Cmd:   t.CMD,
	}

	hc := container.HostConfig{
		RestartPolicy:   rp,
		Resources:       r,
		PublishAllPorts: true,
	}

	resp, err := d.client.ContainerCreate(
		ctx, &cc, &hc, nil, nil, t.ID)
	if err != nil {
		log.Error().Msgf(
			"Error creating container using image %s: %v\n",
			t.Image, err,
		)
		return "", err
	}

	d.mu.Lock()
	d.tasks[t.ID] = resp.ID
	d.mu.Unlock()

	err = d.client.ContainerStart(
		ctx, resp.ID, types.ContainerStartOptions{})
	if err != nil {
		return "", errors.Wrapf(err, "error starting container %s: %v\n", resp.ID, err)
	}

	out, err := d.client.ContainerLogs(
		ctx,
		resp.ID,
		types.ContainerLogsOptions{ShowStdout: true, ShowStderr: true},
	)
	defer func() {
		if err := out.Close(); err != nil {
			log.Error().Err(err).Msgf("error closing stdout on container %s", resp.ID)
		}
	}()
	if err != nil {
		return "", errors.Wrapf(err, "error getting logs for container %s: %v\n", resp.ID, err)
	}
	// limit the amount of data read from stdout to prevent memory exhaustion
	lr := &io.LimitedReader{R: out, N: 1024}
	buf := new(strings.Builder)
	_, err = stdcopy.StdCopy(buf, buf, lr)
	if err != nil {
		return "", errors.Wrapf(err, "error reading the std out")
	}
	statusCh, errCh := d.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return "", err
		}
	case status := <-statusCh:
		log.Debug().
			Int64("status-code", status.StatusCode).
			Str("task-id", t.ID).
			Msg("task completed")
	}
	// remove the container
	if err := d.Stop(ctx, t); err != nil {
		log.Error().Err(err).Str("container-id", resp.ID).Msg("error removing container upon completion")
	}

	return buf.String(), nil
}

func (d *DockerRuntime) Stop(ctx context.Context, t *task.Task) error {
	d.mu.RLock()
	containerID, ok := d.tasks[t.ID]
	d.mu.RUnlock()
	if !ok {
		return nil
	}
	d.mu.Lock()
	delete(d.tasks, t.ID)
	d.mu.Unlock()
	log.Printf("Attempting to stop and remove container %v", containerID)
	return d.client.ContainerRemove(ctx, containerID, types.ContainerRemoveOptions{
		RemoveVolumes: true,
		RemoveLinks:   false,
		Force:         true,
	})
}
