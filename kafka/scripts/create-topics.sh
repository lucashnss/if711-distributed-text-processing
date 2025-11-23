#!/bin/bash
BOOTSTRAP="kafka1:9092"

echo "⏳ Aguardando cluster Kafka iniciar..."
while ! kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; do
  sleep 5
done
echo "✅ Cluster ativo. Recriando tópicos (delete + create)..."

TOPICS=(
  "text-input:12:3"
  "divided_texts:12:3"
  "partial_word_counts:12:3"
  "results:12:3"
)

topic_exists() {
  kafka-topics --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null | grep -Fxq "$1"
}

wait_topic_gone() {
  local name="$1" timeout=60 elapsed=0
  while topic_exists "$name"; do
    (( elapsed++ ))
    if (( elapsed > timeout )); then
      echo "❌ Timeout ao aguardar remoção de $name"
      return 1
    fi
    sleep 1
  done
  return 0
}

for spec in "${TOPICS[@]}"; do
  IFS=":" read -r name partitions replication <<< "$spec"

  if topic_exists "$name"; then
    echo "🗑 Removendo tópico existente: $name"
    kafka-topics --bootstrap-server "$BOOTSTRAP" --delete --topic "$name"
    if ! wait_topic_gone "$name"; then
      echo "⚠ Falha ao remover $name, pulando recriação."
      continue
    fi
    echo "✅ Removido: $name"
  else
    echo "ℹ Tópico $name não existe; será criado."
  fi

  echo "➕ Criando tópico $name (partitions=$partitions replication=$replication)"
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create \
    --topic "$name" --partitions "$partitions" --replication-factor "$replication"
done

echo "🏁 Concluído."
