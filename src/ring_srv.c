// FUNCIONALIDAD DE LA PARTE SERVIDORA
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <pthread.h>
#include "ring.h"
#include "common.h"
#include <limits.h>

static void *request_handler(void *arg);

// función para el thread que implementa la funcionalidad de servidor
// debe recibir como argumento el socket de servicio

void *server_thread(void *arg){
	int s = (long)arg;
	int s_conec;
	socklen_t tam;
	struct sockaddr_in dirCliente;

	while(1){

	  tam = sizeof(dirCliente);
	  s_conec = accept(s, (struct sockaddr *)&dirCliente, &tam);

	  if(s_conec < 0){
		perror("error en accept");
		close(s);
		return NULL;
	  }

	  if(create_thread(request_handler,(void *)(long)s_conec) != 0){
		perror("error en create_thread");
		close(s_conec);
	  }

	}

     return NULL;
}

static void *request_handler(void *arg){
	int soc = (long)arg;
	char op;

	if(recv(soc, &op, sizeof(op), MSG_WAITALL) != sizeof(op)){
		perror("error en recv");
		close(soc);
		return NULL;
	}

	switch(op){

	   case 'P' : {
		int pid_net = htonl(getpid());

		if(send(soc, &pid_net, sizeof(pid_net), 0) != sizeof(pid_net)){
			perror("error en send");
		}

		break;
	   }

	   case 'A':{
		unsigned short puertoNuevo;
		if(recv(soc, &puertoNuevo, sizeof(puertoNuevo), MSG_WAITALL) != (ssize_t)sizeof(puertoNuevo)){
			perror("error en recv");
			break;
		}

		struct sockaddr_in dir;
		socklen_t tam = sizeof(dir);


   		 if(getpeername(soc, (struct sockaddr *)&dir, &tam) < 0){
        		perror("error en getpeername");
        		break;
    		 }

     		 unsigned int ipNueva = dir.sin_addr.s_addr;
    		 unsigned int old_suc_ip;
   		 unsigned short old_suc_port;

   		 if(get_successor(&old_suc_ip, &old_suc_port) < 0){
       			 break;
   		 }

   		 set_successor(ipNueva, puertoNuevo);

   		 if(send(soc, &old_suc_ip, sizeof(old_suc_ip), MSG_MORE) != (ssize_t)sizeof(old_suc_ip)) {
       			 perror("error en send");
       			 break;
   		 }

   		 if(send(soc, &old_suc_port, sizeof(old_suc_port), 0) != (ssize_t)sizeof(old_suc_port)) {
       			 perror("error en send");
   		 }

   	       break;
	    }

	   case 'S': {

		 unsigned int suc_ip;
   		 unsigned short suc_port;

   		 if(get_successor(&suc_ip, &suc_port) < 0){
       			 break;
   		 }

   		 if(send(soc, &suc_ip, sizeof(suc_ip), MSG_MORE) != (ssize_t)sizeof(suc_ip)){
       			 perror("error en send");
       			 break;
   		 }

   		 if (send(soc, &suc_port, sizeof(suc_port), 0) != (ssize_t)sizeof(suc_port)) {
       			 perror("error en send");
    	         }

   		 break;
	   }

	   case 'U': {
   		 unsigned int suc_ip;
   		 unsigned short suc_port;
   		 unsigned int suc_suc_ip;
   		 unsigned short suc_suc_port;

		 if (get_successor(&suc_ip, &suc_port) < 0) {
		        break;
		 }

		 if (ring_remote_successor(suc_ip, suc_port, &suc_suc_ip, &suc_suc_port) < 0) {
		        break;
   		 }

		 if (send(soc, &suc_suc_ip, sizeof(suc_suc_ip), MSG_MORE) != (ssize_t)sizeof(suc_suc_ip)) {
       			 perror("error en send");
       			 break;
   		 }

   		 if (send(soc, &suc_suc_port, sizeof(suc_suc_port), 0) != (ssize_t)sizeof(suc_suc_port)) {
       			 perror("error en send");
   		 }

   		 break;
	   }

           case 'D': {
   		 int longitud_net;

   		 if(recv(soc, &longitud_net, sizeof(longitud_net), MSG_WAITALL) != (ssize_t)sizeof(longitud_net)) {
       			 perror("error en recv");
       			 break;
   		  }

   		 int longitud = ntohl(longitud_net);

		 if(longitud <= 0){
		        int error_net = htonl(-1);
		        send(soc, &error_net, sizeof(error_net), 0);
		        break;
		 }

		 char *nombre = malloc(longitud + 1);
		 if(nombre == NULL) {
		        int error_net = htonl(-1);
		        send(soc, &error_net, sizeof(error_net), 0);
		        break;
		 }

		 if(recv(soc, nombre, longitud, MSG_WAITALL) != (ssize_t)longitud) {
		        perror("error en recv");
		        free(nombre);
		        break;
		 }

		 nombre[longitud] = '\0';

		 int dirfd = get_shared_dir_fd();
		 if(dirfd < 0){
		        int error_net = htonl(-1);
		        send(soc, &error_net, sizeof(error_net), 0);
		        free(nombre);
		        break;
		 }

		 int fd = openat(dirfd, nombre, O_RDONLY);
		 if(fd < 0){
		        int error_net = htonl(-1);
		        send(soc, &error_net, sizeof(error_net), 0);
		        free(nombre);
		        break;
		 }

		 struct stat st;

		 if(fstat(fd, &st) < 0){
		        perror("error en fstat");
		        int error_net = htonl(-1);
		        send(soc, &error_net, sizeof(error_net), 0);
		        close(fd);
		        free(nombre);
		        break;
		 }

		 if(st.st_size > INT_MAX){
		        int error_net = htonl(-1);
		        send(soc, &error_net, sizeof(error_net), 0);
		        close(fd);
		        free(nombre);
		        break;
		 }

		 int tam = st.st_size;
		 int tam_net = htonl(tam);

		 if(send(soc, &tam_net, sizeof(tam_net), tam > 0 ? MSG_MORE : 0) != (ssize_t)sizeof(tam_net)) {
		        perror("error en send");
		        close(fd);
		        free(nombre);
		        break;
		 }

		 off_t offset = 0;

		 while(offset < tam){
		        size_t quedan = tam - offset;
		        ssize_t enviados = sendfile(soc, fd, &offset, quedan);

		        if(enviados <= 0){
		            perror("error en sendfile");
		            break;
		        }
		    }

		    close(fd);
		    free(nombre);

		    break;
	 	}

	case 'L':{
   		 int hops_net;
   		 int longitud_net;

      		 if(recv(soc, &hops_net, sizeof(hops_net), MSG_WAITALL) != (ssize_t)sizeof(hops_net)){
       			 perror("error en recv");
       			 break;
   		 }

   		 if(recv(soc, &longitud_net, sizeof(longitud_net), MSG_WAITALL) != (ssize_t)sizeof(longitud_net)){
       			 perror("error en recv");
       			 break;
   		 }

   		 int hops = ntohl(hops_net);
  		 int longitud = ntohl(longitud_net);

   		 if (hops < 0 || longitud <= 0){
       			 int resultado_net = htonl(-1);
       			 send(soc, &resultado_net, sizeof(resultado_net), 0);
       			 break;
   		 }

   		 char *filename = malloc(longitud + 1);
   		 if(filename == NULL){
       			 int resultado_net = htonl(-1);
       			 send(soc, &resultado_net, sizeof(resultado_net), 0);
      			  break;
   		 }

   		 if(recv(soc, filename, longitud, MSG_WAITALL) != (ssize_t)longitud){
       			 perror("error en recv");
       			 free(filename);
       			 break;
   		 }

   		 filename[longitud] = '\0';

   		 unsigned int ip_encontrada;
   		 unsigned short puerto_encontrado;

   		 if(ring_lookup(filename, hops, &ip_encontrada, &puerto_encontrado) < 0){
       			 int resultado_net = htonl(-1);
       			 send(soc, &resultado_net, sizeof(resultado_net), 0);
       			 free(filename);
       			 break;
   		 }

   		 int resultado_net = htonl(0);

   		 if(send(soc, &resultado_net, sizeof(resultado_net), MSG_MORE) != (ssize_t)sizeof(resultado_net)){
       			 perror("error en send");
       			 free(filename);
       			 break;
   		 }

   		 if(send(soc, &ip_encontrada, sizeof(ip_encontrada), MSG_MORE) != (ssize_t)sizeof(ip_encontrada)){
       			 perror("error en send");
       			 free(filename);
       			 break;
   		 }

   		 if(send(soc, &puerto_encontrado, sizeof(puerto_encontrado), 0) != (ssize_t)sizeof(puerto_encontrado))
       			 perror("error en send");


   		 free(filename);
   		 break;
	   }

	   default:
		fprintf(stderr, "operacion desconocia %c\n", op);
		break;

	}

	close(soc);
	return NULL;
}
