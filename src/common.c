// IMPLEMENTACIÓN DE LA FUNCIONALIDAD COMÚN PARA LA PARTE CLIENTE
// Y LA PARTE SERVIDORA
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "common.h"
#include <fcntl.h>

static pthread_attr_t *atrib_th;
static unsigned int successor_ip;
static unsigned short successor_port;
static pthread_mutex_t successor_mutex = PTHREAD_MUTEX_INITIALIZER;

// crea un thread detached

int create_thread(void *(*func)(void *), void *arg) {
    // prepara atributos adecuados para crear thread "detached"
    if (!atrib_th) {
        atrib_th=malloc(sizeof(pthread_attr_t));
        pthread_attr_init(atrib_th);
        pthread_attr_setdetachstate(atrib_th, PTHREAD_CREATE_DETACHED);
    }
    pthread_t thid;
    return pthread_create(&thid, atrib_th, func, arg);
}

// Inicializa el socket, le asigna un puerto seleccionado por el SO
// y lo prepara para aceptar conexiones. Recibe un parámetro de salida
// donde devuelve el puerto asignado en formato de red.

int create_socket_srv(unsigned short *port) {
    int s;
    struct sockaddr_in addr;
    unsigned int td=sizeof(addr);
    if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        perror("error creando socket"); return -1;
    }
    // Para reutilizar puerto inmediatamente si se rearranca el servidor
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int))<0){
        perror("error en setsockopt"); close(s); return -1;
    }
    // asocia el socket al puerto especificado (si 0, SO elige el puerto)
    addr.sin_addr.s_addr=INADDR_ANY;
    addr.sin_port=0;
    addr.sin_family=PF_INET;
    if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("error en bind"); close(s); return -1;
    }
    if (port) {
        if (getsockname(s, (struct sockaddr *) &addr, &td)<0) {
            perror("error en getsockname"); close(s); return -1;
        }
        *port = addr.sin_port;
    }
    // establece el nº máx. de conexiones pendientes de aceptar
    if (listen(s, 5) < 0) {
        perror("error en listen"); close(s); return -1;
    }
    return s;
}
// crea socket y se conecta al servidor a partir de IP y puerto en formato red
int create_socket_cln(unsigned int ip, unsigned short port) {
    struct sockaddr_in addr = {.sin_family=AF_INET, .sin_port=port, .sin_addr={ip}};
    int s;
    if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
        perror("error creando socket"); return -1;
    }
    // realiza la conexión
    if (connect(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("error en connect"); close(s); return -1;
    }
    return s;
}

void set_successor(unsigned int ip, unsigned short port){
	pthread_mutex_lock(&successor_mutex);
	successor_ip = ip;
	successor_port = port;
	pthread_mutex_unlock(&successor_mutex);
}

int get_successor(unsigned int *ip, unsigned short *port){
	if(ip == NULL || port == NULL)
		return -1;

	pthread_mutex_lock(&successor_mutex);
	*ip = successor_ip;
	*port = successor_port;
	pthread_mutex_unlock(&successor_mutex);
	return 0;
}

static int dirCompart = -1;

int set_shared_dir(const char *dir) {
   	 if(dirCompart >= 0){
        	close(dirCompart);
         }

    	dirCompart  = open(dir, O_RDONLY | O_DIRECTORY);

   	if(dirCompart < 0){
       		 perror("error abriendo directorio compartido");
       		 return -1;
   	 }

    return 0;
}

int get_shared_dir_fd(void) {
    return dirCompart;
}
