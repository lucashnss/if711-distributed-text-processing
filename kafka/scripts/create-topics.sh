#!/bin/bash
echo "⏳ Aguardando cluster Kafka iniciar..."
while ! kafka-broker-api-versions --bootstrap-server kafka1:9092 >/dev/null 2>&1; do
    sleep 5
done
echo "✅ Cluster ativo. Criando tópicos..."

TOPICS=(
  "text-input:3:3"
  "divided_texts:3:3"
  "partial_word_counts:3:3"
  "results:3:3"
)

for t in "${TOPICS[@]}"; do
  IFS=":" read -r name partitions replication <<< "$t"
  kafka-topics --bootstrap-server kafka1:9092 --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor "$replication"
done

echo "✅ Todos os tópicos criados!"
