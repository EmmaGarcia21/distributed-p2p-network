// DECLARACIÓN DE LA FUNCIONALIDAD COMÚN PARA LA PARTE CLIENTE
// Y LA PARTE SERVIDORA
#ifndef _COMMON_H
#define _COMMON_H        1

// thread de servicio
void *server_thread(void *arg);

// crea un thread detached
int create_thread(void *(*func)(void *), void *arg);

// Inicializa el socket, le asigna un puerto seleccionado por el SO
// y lo prepara para aceptar conexiones. Recibe un parámetro de salida
// donde devuelve el puerto asignado.
int create_socket_srv(unsigned short *port);

// crea socket y se conecta al servidor a partir de IP y puerto en formato red
int create_socket_cln(unsigned int ip, unsigned short port);

// guardar y devolver el sucesor del nodo 
void set_successor(unsigned int ip, unsigned short port);
int get_successor(unsigned int *ip, unsigned short *port);

int set_shared_dir(const char *dir);
int get_shared_dir_fd(void);

#endif // _COMMON_H
