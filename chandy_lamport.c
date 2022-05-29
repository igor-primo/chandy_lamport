#include<stdio.h>
#include<stdlib.h>
#include<pthread.h>
#include<mpi.h>
#include<semaphore.h>
#include<unistd.h>
#include<assert.h>

#define Q_SIZ 10	

/* semáforo para gerenciar acesso ao clock
 * pelas threads principal e get_message, onde é
 * implementado o tratamento dos markers */
sem_t semaphore_clock;

/* semáforos para genrenciar acesso à fila
 * de mensagens recebidas pelo processo */
sem_t semaphore;
sem_t full;
sem_t empty;

/* semáforos para genrenciar a fila
 * de mensagens enviadas pelo processo */
sem_t semaphore_output;
sem_t full_output;
sem_t empty_output;

/* tipo de dados para implementar relogio vetorial */
typedef struct clock {
	int p[3];
} CLOCK;

/* tipo de dados para implementar uma mensagem */
typedef struct {
	CLOCK conteudo;
	int source;
	int dest;
	int marker;
} MESSAGE;

/* tipo de dados para implementar as filas */
typedef struct reg {
	MESSAGE		conteudo;
	struct reg	*prox;
} CEL;

/* o relógio vetorial do processo */
CLOCK _clock = {{0,0,0}};

/* variável global para indicar quantos relógios
 * precisam ser processados antes de um marcador 
 * ser processado */
int clocks_to_be_processed = 0;

/* filas para colocar mensagens recebidas,
 * mensagens enviadas e sequências de mensagens 
 * que constituem o estado do canal enquanto
 * o algoritmo de snapshot estiver sendo
 * executado */
CEL 		*input_messages;
CEL			*output_messages;

CEL			*channel0;
CEL			*channel1;
CEL			*channel2;

int			comm_size;
int			my_rank;

/* esse é o relógio vetorial
 * salvo como snapshot */
CLOCK _clock_snapshot;

/* variáveis para auxiliar
 * em redireções de código nas threads principal
 * e get_message */
int SNAPSHOT_STARTED = 0;
int WHO_STARTED_SNAPSHOT = -1;
int MARKERS_SEEN = 0;

/* Funções Fila */
MESSAGE queue_remove(CEL *q);
CEL *queue_insert(MESSAGE m, CEL *q);

/* Funções Produtor/Consumidor */
void produce_input(MESSAGE m);
MESSAGE consume_input();

void produce_output(MESSAGE m);
MESSAGE consume_output();

/* Funções que as threads utilizarão */
void *get_message(void* args);
void *post_message(void* args);

/* Funções do processos */
void *process0();
void *process1();
void *process2();

/* função para processar o relógio vetorial
 * recebido via mensagem */
void update_clock(CLOCK c2);

/* função evento */
void Event(int pid);

/* função executada pelo processo que
 * inicia o snapshot */
void snapshot(int from);

/* tipo de dados para 
 * informar o layout de dados
 * do struct MESSAGE
 * para o MPI */
MPI_Datatype mpi_message_type;

/* Função para mostrar filas */
void show_queue(CEL *q);

int main(int argc, char **argv){
	pthread_t	msg_input;
	pthread_t	rel_vet;
	pthread_t	msg_output;

	/* Semáforos */
	sem_init(&semaphore_clock, 0, 1);

	sem_init(&semaphore, 0, 1);
	sem_init(&full, 0, 0);
	sem_init(&empty, 0, Q_SIZ);

	sem_init(&semaphore_output, 0, 1);
	sem_init(&full_output, 0, 0);
	sem_init(&empty_output, 0, Q_SIZ);

	/* Filas */
	input_messages = (CEL*) malloc(sizeof(CEL));
	input_messages->prox = input_messages;
	output_messages = (CEL*) malloc(sizeof(CEL));
	output_messages->prox = output_messages;

	channel0 = (CEL*) malloc(sizeof(CEL));
	channel0->prox = channel0;

	channel1 = (CEL*) malloc(sizeof(CEL));
	channel1->prox = channel1;

	channel2 = (CEL*) malloc(sizeof(CEL));
	channel2->prox = channel2;

	/* Coisas do MPI */
	MPI_Init(NULL, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &comm_size);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

	/* criacao de tipo para mpi */
	MESSAGE message_t_example;

	int block_c[1] = {3};
	MPI_Datatype type_c[1] = {MPI_INT};
	MPI_Datatype mpi_clock_type;
	MPI_Aint offsets_c[1];
	MPI_Aint base_address_c;

	MPI_Get_address(&message_t_example.conteudo, &base_address_c);
	MPI_Get_address(&message_t_example.conteudo.p, &offsets_c[0]);
	offsets_c[0] = MPI_Aint_diff(offsets_c[0], base_address_c);
	MPI_Type_create_struct(1, block_c, offsets_c, type_c, &mpi_clock_type);
	MPI_Type_commit(&mpi_clock_type);

	int blocks[4] = {1, 1, 1, 1};
	MPI_Datatype types[4] = {mpi_clock_type, MPI_INT, MPI_INT, MPI_INT};
	MPI_Aint offsets[4];
	MPI_Aint base_address;

	MPI_Get_address(&message_t_example, &base_address);
	MPI_Get_address(&message_t_example.conteudo, &offsets[0]);
	MPI_Get_address(&message_t_example.source, &offsets[1]);
	MPI_Get_address(&message_t_example.dest, &offsets[2]);
	MPI_Get_address(&message_t_example.marker, &offsets[3]);

	offsets[0] = MPI_Aint_diff(offsets[0], base_address);
	offsets[1] = MPI_Aint_diff(offsets[1], base_address);
	offsets[2] = MPI_Aint_diff(offsets[2], base_address);
	offsets[3] = MPI_Aint_diff(offsets[3], base_address);

	MPI_Type_create_struct(4, blocks, offsets, types, &mpi_message_type);
	MPI_Type_commit(&mpi_message_type);

	/* Parte que interessa */
	if(my_rank == 0){
		pthread_create(&rel_vet, NULL,
				&process0, NULL);

		/*
		pthread_create(&msg_input, NULL,
				get_message, (void*) 2);
		pthread_create(&msg_output, NULL,
				post_message, (void*) 3);
				*/
		pthread_create(&msg_input, NULL,
				get_message, (void*) 4);
		pthread_create(&msg_output, NULL,
				post_message, (void*) 5);

		pthread_join(msg_input, NULL);
		pthread_join(msg_output, NULL);
	} else if(my_rank == 1){
		pthread_create(&rel_vet, NULL,
				&process1, NULL);

		/*
		pthread_create(&msg_input, NULL,
				get_message, (void*) 2);
		pthread_create(&msg_output, NULL,
				post_message, (void*) 1);
				*/
		pthread_create(&msg_input, NULL,
				get_message, (void*) 4);
		pthread_create(&msg_output, NULL,
				post_message, (void*) 3);

		pthread_join(msg_input, NULL);
		pthread_join(msg_output, NULL);
	} else if(my_rank == 2){
		pthread_create(&rel_vet, NULL,
				&process2, NULL);

		/*
		pthread_create(&msg_input, NULL,
				get_message, (void*) 1);
		pthread_create(&msg_output, NULL,
				post_message, (void*) 1);
				*/
		pthread_create(&msg_input, NULL,
				get_message, (void*) 3);
		pthread_create(&msg_output, NULL,
				post_message, (void*) 3);

		pthread_join(msg_input, NULL);
		pthread_join(msg_output, NULL);
	}

	printf("\n");
	if(my_rank == 0){
		sleep(2);
		printf("In process %d final snapshot\n", my_rank);
		printf("Clock: (%d, %d, %d)\n", _clock_snapshot.p[0], \
				_clock_snapshot.p[1], _clock_snapshot.p[2]);
		printf("Channel 1 ");
		show_queue(channel1);
		printf("Channel 2 ");
		show_queue(channel2);
	} else if (my_rank == 1){
		sleep(4);
		printf("In process %d final snapshot\n", my_rank);
		printf("Clock: (%d, %d, %d)\n", _clock_snapshot.p[0], \
				_clock_snapshot.p[1], _clock_snapshot.p[2]);
		printf("Channel 0 ");
		show_queue(channel0);
		printf("Channel 2 ");
		show_queue(channel2);
	} else if (my_rank == 2){
		sleep(6);
		printf("In process %d final snapshot\n", my_rank);
		printf("Clock: (%d, %d, %d)\n", _clock_snapshot.p[0], \
				_clock_snapshot.p[1], _clock_snapshot.p[2]);
		printf("Channel 0 ");
		show_queue(channel0);
		printf("Channel 1 ");
		show_queue(channel1);
	}

	/* Vá dormir */
	free(input_messages);
	free(output_messages);
	free(channel0);
	free(channel1);
	free(channel2);
	sem_destroy(&semaphore_clock);
	sem_destroy(&semaphore);
	sem_destroy(&full);
	sem_destroy(&empty);
	sem_destroy(&semaphore_output);
	sem_destroy(&full_output);
	sem_destroy(&empty_output);

	MPI_Finalize();

	return 0;
}

MESSAGE queue_remove(CEL *q){
	CEL *p;
	p = q->prox;
	MESSAGE x = p->conteudo;
	q->prox = p->prox;
	free(p);
	return x;
}

CEL *queue_insert(MESSAGE m, CEL *q){
	CEL *new;
	new = malloc(sizeof(CEL));
	new->prox = q->prox;
	q->prox = new;
	q->conteudo = m;
	return new;
}

void produce_input(MESSAGE m){
	sem_wait(&empty);
	sem_wait(&semaphore);
	input_messages = queue_insert(m, input_messages);
	sem_post(&semaphore);
	sem_post(&full);
}

MESSAGE consume_input(){
	sem_wait(&full);
	sem_wait(&semaphore);
	MESSAGE m = queue_remove(input_messages);
	sem_post(&semaphore);
	sem_post(&empty);
	return m;
}

void produce_output(MESSAGE m){
	sem_wait(&empty_output);
	sem_wait(&semaphore_output);
	output_messages = queue_insert(m, output_messages);
	sem_post(&semaphore_output);
	sem_post(&full_output);
}

MESSAGE consume_output(){
	sem_wait(&full_output);
	sem_wait(&semaphore_output);
	MESSAGE m = queue_remove(output_messages);
	sem_post(&semaphore_output);
	sem_post(&empty_output);
	return m;
}

void *get_message(void* args){

	long n_listens = (long) args;

	while(n_listens--){
		printf("Getting a message in process %d\n", my_rank);
		MESSAGE maux;
		MPI_Status status;
		MPI_Recv(&maux, 1, mpi_message_type, MPI_ANY_SOURCE, 
				 MPI_ANY_TAG, MPI_COMM_WORLD, &status);
		maux.source = status.MPI_SOURCE;
		if(!maux.marker){
			if(SNAPSHOT_STARTED 
					&& WHO_STARTED_SNAPSHOT == my_rank){ 
				/* verdadeiro somente para o processo q iniciou
				 * aqui monitoramos os canais de comunicação
				 * e gravamos os relógios comunicados durante o
				 * snapshot
				 */
				if(WHO_STARTED_SNAPSHOT == 0){
					if(maux.source == 1)
						channel1 = queue_insert(maux, channel1);
					else if(maux.source == 2)
						channel2 = queue_insert(maux, channel2);
				} else if(WHO_STARTED_SNAPSHOT == 1){
					if(maux.source == 0)
						channel0 = queue_insert(maux, channel0);
					else if(maux.source == 2)
						channel2 = queue_insert(maux, channel2);
				} else if(WHO_STARTED_SNAPSHOT == 2){
					if(maux.source == 0)
						channel0 = queue_insert(maux, channel0);
					else if(maux.source == 1)
						channel1 = queue_insert(maux, channel1);
				}
			}
			if(MARKERS_SEEN == 1 && WHO_STARTED_SNAPSHOT != my_rank){
				/* Caso em que o processo executor não iniciou o snapshot
				 * e viu um marker pela primeira vez. Também
				 * deve monitorar os canais de entrada. */
				if(my_rank == 0){
					if(maux.source == 1)
						channel1 = queue_insert(maux, channel1);
					else if(maux.source == 2)
						channel2 = queue_insert(maux, channel2);
				} else if(my_rank == 1){
					if(maux.source == 0)
						channel0 = queue_insert(maux, channel0);
					else if(maux.source == 2)
						channel2 = queue_insert(maux, channel2);
				} else if(my_rank == 2){
					if(maux.source == 0)
						channel0 = queue_insert(maux, channel0);
					else if(maux.source == 1)
						channel1 = queue_insert(maux, channel1);
				}
			} else if(MARKERS_SEEN > 1 && WHO_STARTED_SNAPSHOT != my_rank){
				// ???
			}
			sem_wait(&semaphore_clock);
			clocks_to_be_processed++;
			produce_input(maux);
			sem_post(&semaphore_clock);
		} else {
			/* esperar o processo principal processar os relogios
			 * que chegaram antes do marcador. */
			MARKERS_SEEN++;
OUT_OF_HERE:
			sem_wait(&semaphore_clock);
			if(clocks_to_be_processed == 0){
				if(MARKERS_SEEN == 1){
					/* se for o primeiro marcador visto pelo processo,
					 * fazer snapshot do relógio e enviar marcadores para
					 * os outros processos */
					printf("Snapshot in process %d: Clock: (%d, %d, %d)\n", 
							my_rank, _clock.p[0], _clock.p[1], _clock.p[2]);
					_clock_snapshot = _clock;
					for(int i=0;i<3;i++){
						if(i != my_rank){
							MESSAGE m;
							m.source = my_rank;
							m.dest = i;
							m.marker = 1;
							produce_output(m);
						}
					}
				} else if (MARKERS_SEEN > 1){
					// ??????
				}
			} else {
				sem_post(&semaphore_clock);
				goto OUT_OF_HERE;
			}
			sem_post(&semaphore_clock);
		}
	}
	 
}

void *post_message(void* args){

	long n_sends = (long) args;

	while(n_sends--){
		MESSAGE mess = consume_output();
		printf("Sending a message in process %d to %d\n", my_rank, mess.dest);
		MPI_Send(&mess, 1, mpi_message_type, mess.dest, 0, MPI_COMM_WORLD);
		printf("Message sent by %d to %d\n", my_rank, mess.dest);
	}

}

void *process0(){
	Event(0); //(1,0,0)
	printf("Process: %d, Clock: (%d, %d, %d)\n", 0, _clock.p[0], _clock.p[1], _clock.p[2]);
	Event(0); //(2,0,0)
	printf("Process: %d, Clock: (%d, %d, %d)\n", 0, _clock.p[0], _clock.p[1], _clock.p[2]);
	MESSAGE m;
	sem_wait(&semaphore_clock);
	m.conteudo = _clock;
	sem_post(&semaphore_clock);
	m.source = 0;
	m.dest = 1;
	m.marker = 0;
	produce_output(m);
	MESSAGE m2 = consume_input();
	update_clock(m2.conteudo);
	Event(0); //(3,1,0)
	printf("Process: %d, Clock: (%d, %d, %d)\n", 0, _clock.p[0], _clock.p[1], _clock.p[2]);
	Event(0); //(4,1,0)
	printf("Process: %d, Clock: (%d, %d, %d)\n", 0, _clock.p[0], _clock.p[1], _clock.p[2]);
	sem_wait(&semaphore_clock);
	m.conteudo = _clock;
	sem_post(&semaphore_clock);
	m.source = 0;
	m.dest = 2;
	snapshot(0);
	produce_output(m);
	m2 = consume_input();
	update_clock(m2.conteudo);
	Event(0); //(5,1,2)
	printf("Process: %d, Clock: (%d, %d, %d)\n", 0, _clock.p[0], _clock.p[1], _clock.p[2]);
	Event(0); //(6,1,2)
	printf("Process: %d, Clock: (%d, %d, %d)\n", 0, _clock.p[0], _clock.p[1], _clock.p[2]);
	sem_wait(&semaphore_clock);
	m.conteudo = _clock;
	sem_post(&semaphore_clock);
	m.source = 0;
	m.dest = 1;
	produce_output(m);
	Event(0);
	printf("Process: %d, Clock: (%d, %d, %d)\n", 0, _clock.p[0], _clock.p[1], _clock.p[2]);
}

void *process1(){
	Event(1);
    printf("Process: %d, Clock: (%d, %d, %d)\n", 1, _clock.p[0], _clock.p[1], _clock.p[2]);
	MESSAGE m;
	sem_wait(&semaphore_clock);
	m.conteudo = _clock;
	sem_post(&semaphore_clock);
	m.source = 1;
	m.dest = 0;
	m.marker = 0;
	produce_output(m);
	MESSAGE m2 = consume_input();
	update_clock(m2.conteudo);
	Event(1);
    printf("Process: %d, Clock: (%d, %d, %d)\n", 1, _clock.p[0], _clock.p[1], _clock.p[2]);
	m2 = consume_input();
	update_clock(m2.conteudo);
	Event(1);
    printf("Process: %d, Clock: (%d, %d, %d)\n", 1, _clock.p[0], _clock.p[1], _clock.p[2]);
}

void *process2(){
	Event(2);
    printf("Process: %d, Clock: (%d, %d, %d)\n", 2, _clock.p[0], _clock.p[1], _clock.p[2]);
    Event(2);
    printf("Process: %d, Clock: (%d, %d, %d)\n", 2, _clock.p[0], _clock.p[1], _clock.p[2]);
	MESSAGE m;
	sem_wait(&semaphore_clock);
	m.conteudo = _clock;
	sem_post(&semaphore_clock);
	m.source = 2;
	m.dest = 0;
	m.marker = 0;
	produce_output(m);
	MESSAGE m2 = consume_input();
	update_clock(m2.conteudo);
	Event(2);
    printf("Process: %d, Clock: (%d, %d, %d)\n", 2, _clock.p[0], _clock.p[1], _clock.p[2]);
}

void update_clock(CLOCK c2){
	sem_wait(&semaphore_clock);
	clocks_to_be_processed--;
	for(int i=0;i<3;i++)
		if(_clock.p[i] < c2.p[i])
			_clock.p[i] = c2.p[i];
	sem_post(&semaphore_clock);
}

void Event(int pid){
	sem_wait(&semaphore_clock);
    _clock.p[pid]++;    
	sem_post(&semaphore_clock);
}

void snapshot(int from){
	sem_wait(&semaphore_clock);
	SNAPSHOT_STARTED = 1;
	MARKERS_SEEN++;
	WHO_STARTED_SNAPSHOT = from;
	printf("Snapshot in process %d: Clock: (%d, %d, %d)\n", from, 
			_clock.p[0], _clock.p[1], _clock.p[2]);
	_clock_snapshot = _clock;
	for(int i=0;i<3;i++){
		if(i != from){
			MESSAGE m;
			m.marker = 1;
			m.source = from;
			m.dest = i;
			produce_output(m);
		}
	}
	sem_post(&semaphore_clock);
}

void show_queue(CEL *q){
	if(q == q->prox)
		printf("Empty\n");
	else{
		for(CEL *p = q->prox; p != q; p = p->prox)
			printf("[Clock: {%d, %d, %d} | Source: %d | Dest: %d | is_marker: %d] ", \
					p->conteudo.conteudo.p[0], p->conteudo.conteudo.p[1], \
					p->conteudo.conteudo.p[2], p->conteudo.source, p->conteudo.dest, \
					p->conteudo.marker);
		printf("\n");
	}
}
