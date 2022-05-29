# Uma implementação do algoritmo Chandy-Lamport
## Snapshots de sistemas distribuídos

Essa é uma tentativa de implementação do algoritmo
de snapshot Chandy-Lamport para um caso específico,
detalhado na imagem.

Foram utilizadas as bibliotecas pthread padrão de sistemas
baseados Linux e o MPICH.

Compilação: mpicc chandy_lamport.c -o c -lpthread
Execução: mpiexec -n 3 ./c
