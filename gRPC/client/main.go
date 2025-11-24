package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	pb "stopwords_project/frequencia"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Configuração do Teste
const (
	TotalExecucoes = 10000
	EnderecoCentral = "localhost:50051"
)

// Texto Fixo 
var TextoTeste = `
A história da computação é uma jornada fascinante que remonta a milhares de anos, muito antes da invenção do silício ou da eletricidade. No início, a necessidade humana de quantificar, medir e calcular impulsionou a criação de ferramentas rudimentares. O ábaco, surgido na Mesopotâmia, foi um dos primeiros dispositivos criados para auxiliar nas operações matemáticas básicas. No entanto, a verdadeira revolução conceitual começou muito depois, com visionários que sonhavam com máquinas capazes de realizar tarefas muito mais complexas do que simples somas e subtrações.
No século XIX, Charles Babbage, um matemático inglês, projetou a "Máquina Analítica", um dispositivo mecânico que possuía os elementos fundamentais de um computador moderno: uma unidade de processamento, memória e capacidade de entrada e saída de dados. Embora a máquina nunca tenha sido completada durante sua vida devido a limitações tecnológicas da época, as ideias de Babbage lançaram as bases para o futuro. Sua colaboradora, Ada Lovelace, percebeu que a máquina poderia manipular símbolos além de números e escreveu o que é considerado o primeiro algoritmo da história, tornando-se a primeira programadora.
O salto para a era eletrônica ocorreu em meados do século XX, impulsionado pelas necessidades da Segunda Guerra Mundial. Alan Turing, um matemático brilhante, formalizou os conceitos de algoritmo e computação com sua teórica "Máquina de Turing", provando que uma máquina poderia simular qualquer lógica dedutiva. Simultaneamente, a arquitetura de von Neumann foi proposta, definindo o modelo que a maioria dos computadores utiliza até hoje: dados e instruções armazenados na mesma memória. O ENIAC, um colosso de válvulas e cabos, marcou o início da computação eletrônica de propósito geral, capaz de realizar cálculos balísticos em velocidades nunca antes vistas.
A invenção do transistor em 1947 mudou tudo. Substituindo as frágeis e grandes válvulas, os transistores permitiram que os computadores se tornassem menores, mais rápidos, mais baratos e mais confiáveis. Isso levou ao desenvolvimento dos circuitos integrados e, eventualmente, ao microprocessador. A Lei de Moore, observada por Gordon Moore, previu que o número de transistores em um chip dobraria aproximadamente a cada dois anos, uma profecia que impulsionou a indústria por décadas e permitiu a miniaturização exponencial da tecnologia.
Com o hardware evoluindo, o software precisava acompanhar. As primeiras linguagens de programação eram códigos de máquina complexos e difíceis de entender. O surgimento de linguagens de alto nível, como Fortran e COBOL, e mais tarde C, permitiu que programadores escrevessem instruções de forma mais humana e estruturada. O sistema operacional UNIX revolucionou a forma como o software interagia com o hardware, introduzindo conceitos de portabilidade e multitarefa que são a base dos sistemas modernos, incluindo Linux e macOS.
A década de 1970 e 1980 viu o nascimento do computador pessoal. Empresas como Apple e Microsoft trouxeram a computação das salas refrigeradas das universidades e corporações para as mesas de casa e escritórios. A interface gráfica do usuário e o mouse tornaram a tecnologia acessível a não especialistas. De repente, o computador não era apenas uma ferramenta de cálculo, mas uma ferramenta de criação, escrita, design e entretenimento.
No entanto, a maior revolução estava por vir: a interconexão. A ARPANET, um projeto militar, evoluiu para a Internet. Com a criação da World Wide Web por Tim Berners-Lee, a informação tornou-se global e instantânea. Protocolos como TCP/IP padronizaram a comunicação, permitindo que máquinas diferentes conversassem entre si. O mundo encolheu, e a economia global passou a depender inteiramente dessa rede de dados. O e-mail, os navegadores e, posteriormente, as redes sociais transformaram fundamentalmente a sociedade humana.
À medida que entramos no século XXI, a computação enfrentou novos desafios. A Lei de Moore começou a mostrar sinais de saturação; não era mais possível simplesmente aumentar a velocidade do relógio (clock) dos processadores sem gerar calor excessivo. A solução foi a concorrência e o paralelismo. Os processadores passaram a ter múltiplos núcleos (multi-core), exigindo que o software fosse reescrito para executar tarefas simultaneamente. Linguagens modernas como Go (Golang) e Rust foram projetadas especificamente para lidar com essa nova realidade, facilitando a criação de sistemas distribuídos e altamente concorrentes que podem aproveitar todo o poder do hardware moderno.
Hoje, vivemos na era da computação em nuvem e dos dados massivos (Big Data). O processamento não ocorre mais apenas no dispositivo do usuário, mas em vastos centros de dados distribuídos pelo mundo. Arquiteturas de microsserviços e tecnologias como gRPC e Kubernetes permitem que aplicações complexas sejam divididas em partes menores e gerenciáveis, comunicando-se através de redes de alta velocidade. A inteligência artificial e o aprendizado de máquina (Machine Learning) utilizam esse poder computacional massivo para reconhecer padrões, traduzir idiomas e até dirigir carros, coisas que seriam consideradas ficção científica há poucos anos.
A segurança da informação tornou-se uma prioridade crítica. Com tantos dados sensíveis online, a criptografia e a cibersegurança são campos de batalha constantes entre defensores e atacantes. A privacidade dos dados é um tema central de debate ético e legal. Além disso, a computação quântica surge no horizonte como a próxima grande fronteira. Utilizando os princípios da mecânica quântica, como superposição e entrelaçamento, os computadores quânticos prometem resolver problemas químicos, físicos e matemáticos que levariam milhões de anos para serem resolvidos pelos supercomputadores clássicos mais poderosos.
A evolução da computação é uma prova da engenhosidade humana. De contas de argila a qubits quânticos, a busca por processar informações de forma mais rápida e eficiente nunca parou. Cada década trouxe avanços que pareciam impossíveis na anterior. A programação, antes uma tarefa de conectar cabos físicos, tornou-se uma forma de arte lógica e abstrata. O hardware, antes ocupando salas inteiras, agora cabe no bolso ou no pulso.
Para o estudante de ciência da computação ou para o engenheiro de software, entender essa história é fundamental. Não se trata apenas de saber como usar as ferramentas atuais, mas de compreender os problemas fundamentais que essas ferramentas resolvem. A concorrência, por exemplo, não é apenas uma funcionalidade da linguagem Go, mas uma resposta necessária às limitações físicas dos materiais semicondutores. Os sistemas distribuídos não são apenas uma tendência, mas a única maneira de escalar serviços para bilhões de usuários.
O futuro da tecnologia é incerto, mas a tendência de integração e automação é clara. A Internet das Coisas (IoT) promete conectar cada objeto do nosso cotidiano à rede, gerando ainda mais dados e exigindo ainda mais capacidade de processamento em tempo real. A realidade aumentada e virtual prometem mudar a forma como interagimos com o mundo digital. E no centro de tudo isso, está o código. Linhas de texto escritas por seres humanos (e cada vez mais por máquinas) que definem as regras desse novo universo digital.
Portanto, ao estudar programação concorrente, sistemas distribuídos ou compiladores, lembre-se de que você está caminhando sobre os ombros de gigantes. Você é parte de uma linhagem contínua de inovadores que transformaram a areia (silício) em inteligência. A computação é, em última análise, a extensão da mente humana, uma ferramenta para expandir nosso potencial e resolver os problemas mais complexos do nosso tempo. Seja otimizando um algoritmo de ordenação ou arquitetando uma rede neural, cada linha de código contribui para essa grande tapeçaria tecnológica que envolve o planeta. O desafio agora é garantir que essa tecnologia continue a servir à humanidade de forma ética, sustentável e inclusiva para as próximas gerações.
` 

func main() {
	// Conecta ao Central
	conn, err := grpc.NewClient(EnderecoCentral, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("não conectou: %v", err)
	}
	defer conn.Close()

	client := pb.NewAnalisadorClient(conn)

	fmt.Printf("=== Iniciando Benchmark gRPC ===\n")
	fmt.Printf("Invocações: %d\n", TotalExecucoes)
	fmt.Printf("Tamanho do Texto: %d bytes\n", len(TextoTeste))
	fmt.Println("Rodando... (isso pode demorar)")

	// Slice para armazenar o tempo de cada execução (em microssegundos)
	tempos := make([]float64, 0, TotalExecucoes)

	inicioTotal := time.Now()

	for i := 0; i < TotalExecucoes; i++ {
		// VAMOS CAPTURAR O RESULTADO SOMENTE NA PRIMEIRA EXECUÇÃO (i == 0)
		var resp *pb.ResultadoResponse
		var execucaoErr error
        
		inicioReq := time.Now()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
        
        // TROCA: Captura a resposta
		resp, execucaoErr = client.AnalisarTexto(ctx, &pb.TextoRequest{Texto: TextoTeste})
		cancel()

		duracao := time.Since(inicioReq)

		if execucaoErr != nil {
			log.Printf("Erro na execução %d: %v", i, execucaoErr)
			continue
		}
        
        // EXIBE O RESULTADO APENAS UMA VEZ PARA VERIFICAÇÃO
        if i == 0 {
            fmt.Println("\n--- Amostra do Processamento (Primeira Invocação) ---")
            fmt.Printf("Total de Palavras significativas: %d\n", resp.TotalPalavras)
            fmt.Printf("Palavras Únicas: %d\n", resp.PalavrasUnicas)
            fmt.Printf("Top 10: %v\n", resp.GetTopPalavras())
            fmt.Println("-------------------------------------------------------")
        }

		// Armazena em microssegundos
		tempos = append(tempos, float64(duracao.Microseconds()))

		// Barra de progresso simples
		if (i+1)%1000 == 0 {
			fmt.Printf("Completado: %d/%d\n", i+1, TotalExecucoes)
		}
	}

	tempoTotalExecucao := time.Since(inicioTotal)

	// --- Cálculos Estatísticos ---
	var soma float64
	for _, t := range tempos {
		soma += t
	}
	media := soma / float64(len(tempos))

	var somaVariancia float64
	for _, t := range tempos {
		diferenca := t - media
		somaVariancia += (diferenca * diferenca)
	}
	desvioPadrao := math.Sqrt(somaVariancia / float64(len(tempos)))

	// --- Relatório ---
	fmt.Println("\n=== Resultados do Benchmark ===")
	fmt.Printf("Sucesso: %d / %d\n", len(tempos), TotalExecucoes)
	fmt.Printf("Tempo Total do Teste: %v\n", tempoTotalExecucao)
	fmt.Println("--------------------------------")
	fmt.Printf("Média de Latência: %.2f µs\n", media)
	fmt.Printf("Desvio Padrão:     %.2f µs\n", desvioPadrao)
	fmt.Println("--------------------------------")
}