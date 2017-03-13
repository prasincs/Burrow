/* Copyright 2015 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package burrow

import (
	"errors"
	"regexp"
	"strconv"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	"github.com/prasincs/Burrow/protocol"
	"github.com/samuel/go-zookeeper/zk"
)

type SecorClient struct {
	app                *ApplicationContext
	cluster            string
	conn               *zk.Conn
	secorRefreshTicker *time.Ticker
	offsetChannel      chan *protocol.PartitionOffset
	secorGroupLock     sync.RWMutex
}

func NewSecorClient(app *ApplicationContext, cluster string, offsetChannel chan *protocol.PartitionOffset) (*SecorClient, error) {
	// here we share the timeout w/ global zk
	zkconn, _, err := zk.Connect(app.Config.Secor[cluster].Zookeepers, time.Duration(app.Config.Zookeeper.Timeout)*time.Second)
	if err != nil {
		return nil, err
	}

	client := &SecorClient{
		app:            app,
		cluster:        cluster,
		conn:           zkconn,
		secorGroupLock: sync.RWMutex{},
		offsetChannel:  offsetChannel,
	}

	// Now get the first set of offsets and start a goroutine to continually check them
	client.refreshTopics()
	client.secorRefreshTicker = time.NewTicker(time.Duration(client.app.Config.Lagcheck.SecorCheck) * time.Second)
	go func() {
		for _ = range client.secorRefreshTicker.C {
			client.refreshTopics()
		}
	}()

	return client, nil
}

func (secorClient *SecorClient) Stop() {

	if secorClient.secorRefreshTicker != nil {
		secorClient.secorRefreshTicker.Stop()
		secorClient.secorGroupLock.Lock()
		secorClient.secorGroupLock.Unlock()
	}

	secorClient.conn.Close()
}

func parseSecorPartitionId(partitionStr string) (int, error) {
	re := regexp.MustCompile(`^([0-9]+)$`)
	if parsed := re.FindStringSubmatch(partitionStr); len(parsed) == 2 {
		return strconv.Atoi(parsed[1])
	} else {
		return -1, errors.New("Invalid partition id: " + partitionStr)
	}
}

func (secorClient *SecorClient) getOffsetsForPartition(topic string, partition int, partitionPath string) {
	zkNodeStat := &zk.Stat{}
	data, zkNodeStat, err := secorClient.conn.Get(partitionPath)
	if err != nil && err != zk.ErrNodeExists {
		log.Errorf("Node %s doesn't exist. Err: %+v", partitionPath, err)
	}
	switch {
	case err == nil:
		offset, errConversion := strconv.Atoi(string(data))
		switch {
		case errConversion == nil:
			log.Debugf("About to sync Secor offset: [%s,%s]::[%v,%v]\n", topic, partition, offset, zkNodeStat.Mtime)
		}
		partitionOffset := &protocol.PartitionOffset{
			Cluster:   secorClient.cluster,
			Topic:     topic,
			Source:    "secor",
			Partition: int32(partition),
			Group:     "secor_backup-" + topic,
			Timestamp: int64(zkNodeStat.Mtime), // note: this is millis
			Offset:    int64(offset),
		}
		timeoutSendOffset(secorClient.offsetChannel, partitionOffset, 1)
	}
}

func (secorClient *SecorClient) getTopicsPartition(offsetsPath, topic string) {
	topicOffsetPath := offsetsPath + "/" + topic
	partitions, _, err := secorClient.conn.Children(topicOffsetPath)
	switch {
	case err == nil:
		for _, partition_id := range partitions {
			partition, errConversion := parseSecorPartitionId(partition_id)
			switch {
			case errConversion == nil:
				partitionPath := topicOffsetPath + "/" + partition_id
				go secorClient.getOffsetsForPartition(topic, partition, partitionPath)
			default:
				log.Errorf("Something is very wrong! The partition id %s of topic %s in ZK path %s should be a number", partition_id, topic, topicOffsetPath)
			}
		}
	case err == zk.ErrNoNode:
		// it is OK as the offsets may not be managed by ZK
		log.Debugf("No node at %s", offsetsPath)
	default:
		log.Warnf("Failed to read data for topic %s in ZK path %s. Error: %v", topic, topicOffsetPath, err)
	}
}

func (secorClient *SecorClient) refreshTopics() {
	rootPath := secorClient.app.Config.Secor[secorClient.cluster].ZookeeperPath
	offsetsPath := rootPath + "/offsets"
	topics, _, err := secorClient.conn.Children(offsetsPath)
	switch {
	case err == nil:
		for _, topic := range topics {
			go secorClient.getTopicsPartition(offsetsPath, topic)
		}
	case err == zk.ErrNoNode:
		// it is OK as the offsets may not be managed by ZK
		log.Debugf("No node at %s", offsetsPath)
	default:
		log.Warnf("Failed to read data for secor in ZK path %s. Error: %v", rootPath, err)
	}
}
