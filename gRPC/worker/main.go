package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"

	pb "stopwords_project/frequencia"
	"stopwords_project/stopwords"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedAnalisadorServer
}

func (s *server) AnalisarTexto(ctx context.Context, req *pb.TextoRequest) (*pb.ResultadoResponse, error) {
	texto := req.GetTexto()
	// Comentamos o log para não poluir o terminal durante 100.000 execuções
	// log.Printf("Worker: Recebido bloco de texto de tamanho %d", len(texto))

	contagem := make(map[string]int32)
	palavras := strings.Fields(texto)

	for _, p := range palavras {
		limpa := strings.ToLower(strings.TrimFunc(p, func(r rune) bool {
			return strings.ContainsAny(string(r), ".,;:?!()\"'-")
		}))

		if len(limpa) > 0 && !stopwords.IsStopword(limpa) {
			contagem[limpa]++
		}
	}

	return &pb.ResultadoResponse{
		Contagem: contagem,
	}, nil
}

func main() {
	// Flag para definir a porta via linha de comando (Padrão: 50052)
	portPtr := flag.String("port", "50052", "Porta do worker")
	flag.Parse()

	addr := ":" + *portPtr
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("falha ao escutar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterAnalisadorServer(s, &server{})
	fmt.Printf("Worker gRPC rodando na porta %s\n", *portPtr)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("falha ao servir: %v", err)
	}
}