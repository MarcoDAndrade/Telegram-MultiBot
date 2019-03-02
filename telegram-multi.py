#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""Teste de entrega com uso de multipos Bots do Telegram.

Uma classe "Master" recebe uma lista de Bots que vao consumir
duas Queues, uma normal e outra prioritária, fazendo o tratamento
da entrega, retry e priorização.


"""

__author__ = "Marco Andrade <mdacwb@gmail.com>"
__date__ = "20 February 2018"

__version__ = "$Revision: 1 $"

import telegram
from telegram.error import NetworkError, Unauthorized

import threading
import Queue
import time

# SubClass Bot
class Bot():
    """Classe de integracao com Telegram"""
    def __init__(self, name, token, expire=300 ):
        self.name = name
        self.token = token
        self.tg = telegram.Bot( token )
        self.count=0
        self.fail=0
        self.expire = expire

    def send(self, queue, priority=None, limit=None):
        """
        Executa o envio. Se utilizado em DEBUG, acrescentar o parametro limit=N.
        Para encerrar, enviar a string ":quit" no queue.
        """
        logging.info("Iniciando Bot dispatcher")
        try:
            self.update_id = self.tg.get_updates()[0].update_id
        except IndexError:
            self.update_id = None
        if not priority and not queue:
            logging.error("Bot nao iniciado! Nenhum queue informado")
            return

        while True:
            item = None

            # Inicializar queue, mais simples que multiplas validacoes
            if not priority:
                priority = Queue.Queue()

            while ( not queue.empty() or not priority.empty() ):
                try:
                    #-- Se tiver prioridade
                    try:
                        if not priority.empty() and priority.qsize() > 0:
                            logging.info( "Buscando item prioritario" )
                            msg = priority.get(1)
                            priority.task_done()
                        else:
                            logging.info( "Buscando item normal" )
                            msg = queue.get()
                            queue.task_done()
                    except Exception as e:
                        logging.warning( "Bypass de falha ao pegar item do Queue: %s - %s" % ( type(e), e.message ) )
                        next


                    if item == ':quit':
                        logging.info(" -> QUIT")
                        break

                    if not 'retry' in msg:
                        msg['retry']=0

                    if time.time()-msg['create'] > self.expire:
                        logging.warning("Mensagem expirada: %s para: %s" % ( msg['message'], msg['chat'] ) )
                        next;

                    logging.info("Tratando mensagem: %s para: %s" % ( msg['message'], msg['chat'] ) )

                    logging.info(" -> Send: %s" % str(msg) )
                    self.tg.send_message(chat_id=msg['chat'], text=msg['message'])
                    logging.info(" -> Ok")
                    self.count += 1

                except Exception as e:
                    if msg['retry'] < 3:
                        msg['retry'] += 1
                        priority.put( msg )
                        logging.warning(
                                'bot=%s Retry %s ao enviar: %s - %s:%s' % ( self.name, msg['retry'], str(msg), type(e), e.message )
                            )
                    else:
                        logging.warning(
                                'bot=%s Falha ao enviar: %s - %s:%s' % ( self.name, str(msg), type(e), e.message )
                            )
                    logging.warning("> Bot %s entrando em delay de 5 segundos!" % self.name )
                    time.sleep(5)
                    self.fail += 1

                logging.info(" . Done")

                if not limit == None:
                    if not limit:
                        break
                    limit -= 1

            if not limit == None and not limit:
                break
            if item == ':quit':
                break
            time.sleep(0.01)

        logging.info("Encerrando Bot dispatcher")


    def summary(self):
        """ Retorna resumo de mensagens com sucesso/falha. """
        return " %5s %-10s (%s success/%s fail)" % ( self.count+self.fail, self.name, self.count, self.fail )



class Bots():
    """Classe Mestre, que faz o controle de multiplos Bots de entrega"""
    def __init__(self, expire=300):
        """Por padrao, definida uma validade de 5 minutos para envio das mensagens"""
        self.Bots = dict()
        self.Count = 0
        self.queue = Queue.Queue()
        self.priority = Queue.Queue()
        self.Next = list()
        self.expire = expire
        self.start = time.time()

    def add(self, name, token, expire=None, debug=None):
        """Criacao de um novo Bot de entrega.
        Para depuracao, passar valor para o parametro "debug", e acionar diretamente este metodo.
        """

        if name in self.Bots:
            print "ERRO: bot %s ja adicionado!" % name
            return None

        if not expire:
            expire=self.expire

        self.Bots[name] = Bot( name=name, token= token, expire=expire )
        if self.Bots[name]:
            self.Next.append( name )

        self.Count += 1

        if not debug:
            worker = threading.Thread(target=self.Bots[name].send, name='thread.bot:%s'%name, args=(self.queue, self.priority, ) )
            worker.setDaemon(True)
            worker.start()
        else:
            logging.info("Iniciando em modo DEBUG. Acionar pontualmente!")



    def send(self, chat, message, create=time.time()):
        """Inclui mensagem para entrega, na queue padrao."""
        self.queue.put( dict(chat=chat, message=message, create=create, retry=0 ) )

    def priority(self, chat, message, create=time.time()):
        """Inclui mensagem para entrega, na queue prioritaria."""
        self.priority.put( dict(chat=chat, message=message, create=create, retry=0 ) )

   # def __del__(self):
   #     print("Resumo:")
   #     for k in self.Bots:
   #         print("  %10s: %3s Envios e %s falhas" % ( k, self.Bots[k]['count'], self.Bots[k]['fail'] ) )


    def summary(self):
        """Sumarizacao das entregas feitas pelos Bots"""
        summary = list()
        summary.append('Totalizacao de envios')
        for name in self.Names():
            summary.append( self.Bots[name].summary() )

        return "\n".join(summary)

    def Names(self):
        return self.Bots.keys()


    def Finish(self):
        """Envia sinal de encerramento aos Bots e aguarda liberacao da fila"""
        retry=3
        fatal=10
        logging.info( "Join para aguardar limpar a fila" )
        self.queue.join()
        while fatal > 0:
            while not self.queue.empty() and retry > 0:
                for bot in self.Bots.keys():
                    self.queue.put( ':quit' )
                time.sleep(1)
                retry -= 1
            if not self.queue.empty():
                logging.warning( "Aguardando %s itens no quue!" % self.queue.qsize() )
            fatal -= 1
            time.sleep(1)
        if not self.queue.empty():
            logging.error( "Restaram %s itens no quue!" % self.queue.qsize() )



if __name__ == '__main__':

    import logging
    logging.basicConfig( format='%(asctime)s - (%(threadName)-10s) - %(name)s - %(levelname)s - %(message)s', level=logging.INFO )
    #logging.basicConfig( filename='log', format='%(asctime)s - (%(threadName)-10s) - %(name)s - %(levelname)s - %(message)s', level=logging.INFO )


    logging.info("Inicializando Bots")
    botList = Bots(1)

    botList.add('p2t1_bot', '668610900:AAG0IUITwC0_cP25P0m5BwIi2pcBl7Rw7F8')
    botList.add('p2t2_bot', '745518194:AAHLMpF-kSNksq0KDvmYSit3a0nUDkobnmM')
    botList.add('p2t3_bot', '693764351:AAFx1rW312UP8NnqbJR7LXattowtTKaP-jc')
    botList.add('p2t4_bot', '713129482:AAHv7pCc6TNL8B8PWO9P6GlO-DiaybtTX54')

    chats = [ -373130885, -335116004, -314421007, -262985433, -336224985 ]

    CountTo = 100
    Count = 0

    logging.info("Loop de novas mensagens")
    while Count < CountTo:

        chat=chats.pop()
        chats.insert(0, chat)

        Count += 1
        logging.info("- Inserindo mensagem %s/%s em fila" % ( Count, CountTo ) )

        botList.send( chat=chat, message="It's a %s/%s message" % ( Count, CountTo ) )


    #for name in botList.Bots:
    #    botList.Bots[name].send(botList.queue, botList.priority, limit=5 )

    logging.info("Finalizando")
    botList.Finish()

    logging.info("Encerrando tudo")

    logging.info( botList.summary() )
