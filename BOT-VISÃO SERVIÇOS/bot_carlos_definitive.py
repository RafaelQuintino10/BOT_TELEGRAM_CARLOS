from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import psycopg2
import asyncio

active_groups = {}  # ARMAZENA OS GRUPOS ATIVOS 

# APRESENTAÇÃO DO BOT (OPCIONAL)
async def iniciar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(''' 
Olá! Eu sou Visão Serviços, o seu assistente virtual! 
Como posso ajudá-lo(a)?
- **/disparar**: Inicia o disparo periódico de mensagens. ''')


# FUNÇÃO PARA MONITORAR OS GRUPOS. SE ALGUM GRUPO NOVO INTERAGIR COM O BOT, ELE SERÁ ADICIONADO AQUI.
async def monitorar_grupo(chat_id, chat_nome, context):
    try:
        # CONEXÃO COM O BANCO DE DADOS. 
        conexao = psycopg2.connect( 
            host="localhost",
            database='bot_db',
            user='postgres',
            password='fla1357912',
            port='5432'
        )
        cursor = conexao.cursor()
        # INSERE NA TABELA 'TLG_GRUPOS' O NOME E O CHAT ID DE CADA GRUPO QUE INTERAGIR COM O BOT. O MESMO VALE PARA CHATS PRIVADOS.
        cursor.execute(''' INSERT INTO TLG_GRUPOS (COD_GRUPO,NOMEGRUPO) VALUES (%s,%s) ''', (chat_id, chat_nome))
        conexao.commit()
        
        print(f'Conexão estabelecida para {chat_nome} com sucesso!')
        
        while True:
            try:
                # SELECIONA TODAS AS MENSAGENS DA TABELA QUE ESTIVEREM COM PARÂMETRO  'ENVIADA' IGUAL A 0. 
                cursor.execute('SELECT * FROM TLG_NOTIFICACOES WHERE ENVIADA = 0 ORDER BY DTNOTIFIC ASC')
                dados = cursor.fetchall()
                if dados:
                    for linha in dados:
                        # DESDESTRUTURAÇÃO SEM ÍNDICES. 
                        COD_GRUPO, EMPR, CODFUN, DTNOTIFIC, DSNOTIFIC, ENVIADA, TIMELOG = linha
                        # AGUARDA O TEMPO ESTABELECIDO NA COLUNA 'DTNOTIFIC' PARA ENVIAR AS MENSAGENS.
                        await asyncio.sleep(int(DTNOTIFIC))
                        # ENVIO DAS MENSAGENS ARMAZENADA NA VARIÁVEL DSNOTIFIC PARA O CHAT DO TELEGRAM.
                        await context.bot.send_message(chat_id=chat_id, text=DSNOTIFIC)
                        # ATUALIZA O PARÂMETRO 'ENVIADA' PARA 1 QUANDO A RESPECTIVA MENSAGEM FOR ENVIADA NO CHAT.
                        # TAMBÉM ADICIONA NA COLUNA 'TIMELOG' O HORÁRIO QUE CADA MENSAGEM FOI ENVIADA.
                        cursor.execute('UPDATE TLG_NOTIFICACOES SET ENVIADA = 1, TIMELOG =  NOW()::timestamp(0) WHERE DSNOTIFIC = %s', (DSNOTIFIC,))
                        conexao.commit()
                else:
                    print(f'Nenhuma notificação a ser enviada para {chat_nome}.')
                    await asyncio.sleep(2)
            except Exception as error:
                # LOG DE ERROS DE CONEXÃO.
                print(f'Erro ao consultar ou atualizar os dados: {error}')
                await context.bot.send_message(chat_id=chat_id, text=f'Erro ao consultar ou atualizar os dados: {error}')
    # LOG DE ERROS DE CONEXÃO.
    except Exception as error:
        print(f'Erro ao conectar ao banco de dados: {error}')
        await context.bot.send_message(chat_id=chat_id, text=f'Erro ao conectar ao banco de dados: {error}')
    finally:
        if cursor:
            cursor.close()
        if conexao:
            conexao.close()
            print(f'Conexão encerrada para {chat_nome}.')

# FUNÇÃO PARA INICIAR O DISPARO DAS MENSAGENS. 
async def disparar(update: Update, context: ContextTypes.DEFAULT_TYPE):

    chat_id = update.message.chat_id

    # VERICIDA SE O CHAT É UM GRUPO OU UMA CONVERSA PRIVADA.
    if update.message.chat.type in ['group', 'supergroup']:
        chat_nome = update.message.chat.title
    else:
        chat_nome = update.message.chat.first_name or update.message.chat.username or "Usuário"

    if chat_id not in active_groups:
        active_groups[chat_id] = asyncio.create_task(monitorar_grupo(chat_id, chat_nome, context))
        await update.message.reply_text('Monitoramento iniciado!')
    else:
        await update.message.reply_text('Já estou monitorando este grupo!')

# FUNÇÃO PRINCIPAL QUE CONTROLA OS COMANDOS DO BOT.
def main():
    conexao_api = Application.builder().token("8012171445:AAFK183HpQe5DfDOUvduPUyxqvKThQ1NFlc").build()
    # COMANDOS PROGRAMADOS NO BOT: /ola e /iniciar. O SERVE PARA O BOT SE APRESENTAR(OPCIONAL), O SEGUNDO SERVE PARA INICIAR O DISPARP E MONITORAMENTO DOS GRUPOS. OS COMANDO DEVEM SER ENVIADOS NO MODELO NO QUAL FORAM DEMONSTRADOS: PRECEDIDO DE '/' E EM MINÚSCULO, EXEMPLO: /disparar.
    conexao_api.add_handler(CommandHandler('ola', iniciar))
    conexao_api.add_handler(CommandHandler('disparar', disparar))

    conexao_api.run_polling()

if __name__ == '__main__':
    main()



