package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	producer := NewKafkaProducer()
	
	// sem key e sem deliveryChan
	// Publish("mensagem", "testego", producer, nil)

	// com deliveryChan
	deliveryChan := make(chan kafka.Event)
	Publish("mensagem-com-deliveryChan-async-key-for-consumer1", "testego", producer, []byte("transferencia"), deliveryChan)
	// fmt.Println("teste-async")


	// pegando o resultado do envio da msg com o deliveryChan
	// e := <-deliveryChan
	// msg := e.(*kafka.Message)
	// if msg.TopicPartition.Error != nil {
	// 	fmt.Println("Erro ao enviar")
	// } else {
	// 	fmt.Println("Mensagem enviada:", msg.TopicPartition)
	// }
	
	//necessário para pode dar tempo da msg ser processada e enviada, não necessario no modo async
	producer.Flush(5000)

	// pegando o retorno do recebimento da msg de forma assincrona	
	go DeliveryReport(deliveryChan) // async
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "fc2-gokafka-kafka-1:9092",
		"delivery.timeout.ms": "0", //0 = infinito
		"acks":                "all",
		"enable.idempotence":  "true", // obs: se a idempotencia = true, então o acks = all
	}
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}

// Sem deliveryChan
// func Publish(msg string, topic string, producer *kafka.Producer, key []byte) error {
// 	message := &kafka.Message{
// 		Value:          []byte(msg),
// 		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
// 		Key:            key,
// 	}
// 	err := producer.Produce(message, nil)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }



// Com deliveryChan
func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mensagem enviada:", ev.TopicPartition)
				// anotar no banco de dados que a mensagem foi processado.
				// ex: confirma que uma transferencia bancaria ocorreu.
			}
		}
	}
}
