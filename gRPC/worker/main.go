package main

import (
	"context"
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
	log.Printf("Worker: Recebido bloco de texto de tamanho %d", len(texto))

	contagem := make(map[string]int32)
	palavras := strings.Fields(texto)

	for _, p := range palavras {
		// Limpeza simples
		limpa := strings.ToLower(strings.TrimFunc(p, func(r rune) bool {
			return strings.ContainsAny(string(r), ".,;:?!()\"'-")
		}))

		if len(limpa) > 0 && !stopwords.IsStopword(limpa) {
			contagem[limpa]++
		}
	}

	// Worker retorna apenas o mapa preenchido, o resto o Central calcula
	return &pb.ResultadoResponse{
		Contagem: contagem,
	}, nil
}

func main() {
	// Worker roda na porta 50052
	port := ":50052"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("falha ao escutar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterAnalisadorServer(s, &server{})
	log.Printf("Worker gRPC pronto em %s", port)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("falha ao servir: %v", err)
	}
}