// FUNCIONALIDAD DE LA PARTE CLIENTE
#include <sys/mman.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include "ring.h"
#include "common.h"

static int is_initialized(void);
static int initialize(void);
static char *dirCompartido;
static unsigned int ipLocal;
static unsigned short puerto;
static int service_socket = -1;

// inicia el nodo añadiéndolo a la red P2P si ya está creada;
// los puertos e IPs deben estar en formato de red;
// debe devolver en el último parámetro el puerto reservado en formato red;
// retorna 0 si OK y -1 si error

int ring_init(const char *shrd_dir, unsigned int local_ip, unsigned int remote_ip, unsigned short remote_port, unsigned short *alloc_port) {
    if (initialize())
	 return -1; // ya está inicializada

    dirCompartido = strdup(shrd_dir);
    if(dirCompartido == NULL)
	return -1;

    if(set_shared_dir(shrd_dir) < 0){
	return -1;
    }

    ipLocal = local_ip;
    service_socket = create_socket_srv(&puerto);
    if(service_socket < 0)
	return -1;

    if(alloc_port != NULL)
	*alloc_port = puerto;

    if(create_thread(server_thread,(void *)(long)service_socket) != 0){
	close(service_socket);
	service_socket = -1;
	return  -1;
    }

    if(remote_ip == 0){

	set_successor(ipLocal,puerto);

    }else{

	int s = create_socket_cln(remote_ip, remote_port);

	if(s < 0)
	   return -1;

	char op = 'A';

	if(send(s, &op, sizeof(op), MSG_MORE) != (ssize_t)sizeof(op)){
		perror("error en send");
		close(s);
		return -1;
	}

	if(send(s, &puerto, sizeof(puerto), 0) != (ssize_t)sizeof(puerto)){
		perror("error en send");
		close(s);
		return -1;
	}

	unsigned int suc_ip;
	unsigned short suc_port;

	if(recv(s, &suc_ip, sizeof(suc_ip), MSG_WAITALL) !=  (ssize_t)sizeof(suc_ip)){
		perror("error en recv");
		close(s);
		return -1;
	}

	if(recv(s, &suc_port, sizeof(suc_port), MSG_WAITALL) !=  (ssize_t)sizeof(suc_port)){
		perror("error en recv");
		close(s);
		return -1;
	}

	close(s);
	set_successor(suc_ip, suc_port);
    }

  return 0;

}

// función local que devuelve la IP y el puerto del nodo;
// retorna 0 si OK y -1 si error

int ring_self(unsigned int *ip, unsigned short *port) {
    if (!is_initialized())
	 return -1; // no está inicializada

    if(ip == NULL || port == NULL)
	 return -1;

    *ip = ipLocal;
    *port = puerto;

     return 0;
}

// devuelve el PID del nodo remoto especificado o -1 si error

int ring_remote_pid(unsigned int remote_ip, unsigned short remote_port) {
    if (!is_initialized())
	 return -1; // no está inicializada

    int s = create_socket_cln(remote_ip, remote_port);

    if(s < 0)
	return -1;

    char op = 'P';
    if(send(s, &op, sizeof(op), 0) != sizeof(op)){
	perror("error en send");
	close(s);
	return -1;
     }

    int pid_net;
    if(recv(s, &pid_net, sizeof(pid_net), MSG_WAITALL) != sizeof(pid_net)){
	perror("error en recv");
	close(s);
	return -1;
    }

    close(s);
    return ntohl(pid_net);
}

// función local que devuelve la IP y el puerto del nodo sucesor;
// retorna 0 si OK y -1 si error

int ring_successor(unsigned int *ip, unsigned short *port) {
    if (!is_initialized())
	 return -1; // no está inicializada
    return get_successor(ip,port);
}

// devuelve la IP y el puerto del nodo sucesor del especificado;
// retorna 0 si OK y -1 si error

int ring_remote_successor(unsigned int remote_ip, unsigned short remote_port, unsigned int *suc_ip, unsigned short *suc_port) {
    if (!is_initialized())
	 return -1; // no está inicializada

    if(suc_ip == NULL || suc_port == NULL)
	return -1;

    int s = create_socket_cln(remote_ip, remote_port);

    if(s < 0)
	return -1;

    char op = 'S';

    if(send(s, &op, sizeof(op), 0) != (ssize_t)sizeof(op)){
	perror("error en send");
	close(s);
	return -1;
    }

    if(recv(s, suc_ip, sizeof(*suc_ip), MSG_WAITALL) != (ssize_t)sizeof(*suc_ip)){
	perror("error en recv");
	close(s);
	return -1;
    }


    if(recv(s, suc_port, sizeof(*suc_port), MSG_WAITALL) != (ssize_t)sizeof(*suc_port)){
	perror("error en recv");
	close(s);
	return -1;
    }


   close(s);
   return 0;
}

// devuelve la IP y el puerto del nodo sucesor del sucesor del especificado;
// retorna 0 si OK y -1 si error

int ring_remote_successor_successor(unsigned int remote_ip, unsigned short remote_port, unsigned int *suc_suc_ip, unsigned short *suc_suc_port) {
    if (!is_initialized())
	 return -1; // no está inicializada

    if(suc_suc_ip == NULL || suc_suc_port == NULL)
	return -1;

    int s = create_socket_cln(remote_ip,remote_port);
    if( s < 0)
	return -1;

    char op = 'U';

    if(send(s, &op, sizeof(op), 0) != (ssize_t)sizeof(op)){
	perror("error en send");
	close(s);
	return -1;
    }

    if(recv(s, suc_suc_ip, sizeof(*suc_suc_ip), MSG_WAITALL) != (ssize_t)sizeof(*suc_suc_ip)){
	perror("error en recv");
	close(s);
	return -1;
    }

    if(recv(s, suc_suc_port, sizeof(*suc_suc_port), MSG_WAITALL) != (ssize_t)sizeof(*suc_suc_port)){
	perror("error en recv");
	close(s);
	return -1;
    }

   close(s);
   return 0;
}

// descarga el fichero del nodo especificado;
// retorna el tamaño del fichero si OK y -1 en caso de error

int ring_download(unsigned int remote_ip, unsigned short remote_port, const char *filename) {
    if(!is_initialized())
	 return -1; // no está inicializada

    if (filename == NULL)
	return -1;

    int s = create_socket_cln(remote_ip, remote_port);
    if(s < 0)
	 return -1;

    char op = 'D';

    int longitud = strlen(filename);
    int longitud_net = htonl(longitud);

    if(send(s, &op, sizeof(op), MSG_MORE) != (ssize_t)sizeof(op)){
        perror("error en send");
        close(s);
        return -1;
    }

    if(send(s, &longitud_net, sizeof(longitud_net), MSG_MORE) != (ssize_t)sizeof(longitud_net)) {
        perror("error en send");
        close(s);
        return -1;
    }

    if(send(s, filename, longitud, 0) != (ssize_t)longitud){
        perror("error en send");
        close(s);
        return -1;
    }

    int tam_net;

    if(recv(s, &tam_net, sizeof(tam_net), MSG_WAITALL) != (ssize_t)sizeof(tam_net)){
        perror("error en recv");
        close(s);
        return -1;
    }

    int tam = ntohl(tam_net);

    if(tam < 0){
        close(s);
        return -1;
    }

    int dirfd = get_shared_dir_fd();
    if(dirfd < 0){
        close(s);
        return -1;
    }

    int fd = openat(dirfd, filename, O_CREAT | O_TRUNC | O_RDWR, 0666);
    if(fd < 0){
        perror("error en openat");
        close(s);
        return -1;
    }

    if(ftruncate(fd, tam) < 0){
        perror("error en ftruncate");
        close(fd);
        close(s);
        return -1;
    }

    if(tam == 0){
        close(fd);
        close(s);
        return 0;
    }

    char *p = mmap(NULL, tam, PROT_WRITE, MAP_SHARED, fd, 0);
    if(p == MAP_FAILED){
        perror("error en mmap");
        close(fd);
        close(s);
        return -1;
    }

    close(fd);

    if(recv(s, p, tam, MSG_WAITALL) != (ssize_t)tam){
        perror("error en recv");
        munmap(p, tam);
        close(s);
        return -1;
    }

    munmap(p, tam);
    close(s);

    return tam;
}

static int fichero_local_existe(const char *filename){
    int dirfd = get_shared_dir_fd();
    if(dirfd < 0)
	 return 0;

    int fd = openat(dirfd, filename, O_RDONLY);
    if(fd < 0)
	return 0;

    close(fd);
    return 1;
}

// busca el fichero en el anillo dando un número máximo de saltos y devolviendo
// la IP y el puerto del nodo que lo contiene;
// retorna 0 si OK y -1 si error

int ring_lookup(const char *filename, int hops, unsigned int *ip, unsigned short *port) {
    if (!is_initialized())
	return -1; // no está inicializada

    if (filename == NULL || ip == NULL || port == NULL) 
	return -1;

    if (hops < 0)
	return -1;

    if(fichero_local_existe(filename)){
        *ip = ipLocal;
        *port = puerto;
        return 0;
    }

    if(hops == 0)
        return -1;

    unsigned int suc_ip;
    unsigned short suc_port;

    if(get_successor(&suc_ip, &suc_port) < 0)
        return -1;

    int s = create_socket_cln(suc_ip, suc_port);
    if(s < 0)
        return -1;

    char op = 'L';

    int hops_net = htonl(hops - 1);
    int longitud = strlen(filename);
    int longitud_net = htonl(longitud);

    if(send(s, &op, sizeof(op), MSG_MORE) != (ssize_t)sizeof(op)){
        perror("error en send");
        close(s);
        return -1;
    }

    if(send(s, &hops_net, sizeof(hops_net), MSG_MORE) != (ssize_t)sizeof(hops_net)){
        perror("error en send");
        close(s);
        return -1;
    }

    if(send(s, &longitud_net, sizeof(longitud_net), MSG_MORE) != (ssize_t)sizeof(longitud_net)){
        perror("error en send");
        close(s);
        return -1;
    }

    if(send(s, filename, longitud, 0) != (ssize_t)longitud){
        perror("error en send");
        close(s);
        return -1;
    }

    int resultado_net;

    if(recv(s, &resultado_net, sizeof(resultado_net), MSG_WAITALL) != (ssize_t)sizeof(resultado_net)){
        perror("error en recv");
        close(s);
        return -1;
    }

    int resultado = ntohl(resultado_net);

    if(resultado < 0){
        close(s);
        return -1;
    }

    if(recv(s, ip, sizeof(*ip), MSG_WAITALL) != (ssize_t)sizeof(*ip)){
        perror("error en recv");
        close(s);
        return -1;
    }

    if(recv(s, port, sizeof(*port), MSG_WAITALL) != (ssize_t)sizeof(*port)){
        perror("error en recv");
        close(s);
        return -1;
    }

    close(s);
    return 0;
}

// busca y descarga el fichero del nodo encontrado en el anillo que lo contiene;
// retorna el tamaño del fichero si OK y -1 en caso de error;
// ESTA FUNCIÓN YA ESTA COMPLETADA

int ring_get_file(const char *filename, int hops) {
    if (!is_initialized()) return -1; // no está inicializada
    unsigned int ip, ip_local;
    unsigned short port, port_local;
    int res = ring_lookup(filename, hops, &ip, &port);
    ring_self(&ip_local, &port_local);
    // realiza la descarga si encontrado en un nodo que no es el local
    if ((res!=-1) && ((ip!=ip_local) || (port!=port_local)))
        res = ring_download(ip, port, filename);
    return res;
}

// funciones auxiliares

static int initialized;

static int initialize(void) {
    return initialized?1:(initialized=1,0);
}
static int is_initialized(void) {
    return initialized;
}
