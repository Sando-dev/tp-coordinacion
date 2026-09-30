# Coordinación entre los Sum

Las distintas réplicas de Sum consumen de la misma cola, entonces los mensajes de un mismo cliente pueden terminar siendo procesados por distintas instancias. Por eso cada Sum guarda resultados parciales separados por clientId.

Cuando una réplica recibe el EOF de un cliente, lo publica en un exchange de coordinación para avisarle al resto de los Sum que ese cliente terminó de enviar datos. Cada réplica recibe ese EOF por su propia routing key y, al procesarlo, envía sus resultados parciales a Aggregation.

Para evitar que un Sum haga el flush antes de terminar un mensaje que todavía estaba procesando, se usa Qos(1) junto con un mutex compartido entre el procesamiento de datos y el del EOF. Con Qos(1), RabbitMQ entrega como máximo un mensaje sin ACK a cada consumidor, y el mutex hace que el EOF espere si todavía hay un mensaje de datos siendo procesado.

Elegí Qos(1) porque con un prefetch mayor podrían quedar varios mensajes ya entregados pero todavía pendientes cuando llegue el EOF desde otro Sum. En ese caso, el mutex solo alcanzaría para esperar al mensaje que se está procesando en ese momento, pero no garantizaría que se procesen los demás antes del flush. Resolver eso requeriría agregar otro mecanismo para llevar cuenta o drenar los mensajes pendientes.

No supe encontrar una solución para un caso donde Qos sea mayor a 1.

Después, cada Sum distribuye sus resultados hacia las réplicas de Aggregation usando un hash de (clientId, fruit), de forma que los datos de una misma fruta y cliente siempre lleguen a la misma réplica.