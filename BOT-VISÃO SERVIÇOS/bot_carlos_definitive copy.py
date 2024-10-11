from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import psycopg2
import asyncio

active_groups = {}  # Armazena grupos ativos


# Aprecen
async def iniciar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(''' 
Olá! Eu sou Visão Serviços, o seu assistente virtual! 
Como posso ajudá-lo(a)?
- **/disparar**: Inicia o disparo periódico de mensagens.

                                    ... ''')

async def voltar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await iniciar(update, context)

async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(''' 🔍 **Sobre mim**: ... ''')

async def monitorar_grupo(chat_id, chat_nome, context):
    try:
        conexao = psycopg2.connect(
            host="192.168.88.237",
            database='visao_ponto_db',
            user='postgres',
            password='QamN0*yxe7!',
            port='5432'
        )
        cursor = conexao.cursor()
        cursor.execute(''' INSERT INTO TLG_GRUPOS (COD_GRUPO,NOMEGRUPO) VALUES (%s,%s) ''', (chat_id, chat_nome))
        conexao.commit()
        
        print(f'Conexão estabelecida para {chat_nome} com sucesso!')
        
        while True:
            try:
                cursor.execute('SELECT * FROM TLG_NOTIFICACOES WHERE ENVIADA = 0 ORDER BY DTNOTIFIC ASC')
                dados = cursor.fetchall()
                if dados:
                    for linha in dados:
                        # Desestruturação sem índices
                        COD_GRUPO, EMPR, CODFUN, DTNOTIFIC, DSNOTIFIC, ENVIADA, TIMELOG = linha

                        await asyncio.sleep(int(DTNOTIFIC))

                        await context.bot.send_message(chat_id=chat_id, text=DSNOTIFIC)
                        
                        cursor.execute('UPDATE TLG_NOTIFICACOES SET ENVIADA = 1, TIMELOG =  NOW()::timestamp(0) WHERE DSNOTIFIC = %s', (DSNOTIFIC,))
                        conexao.commit()
                else:
                    print(f'Nenhuma notificação a ser enviada para {chat_nome}.')
                    await asyncio.sleep(2)
            except Exception as error:
                print(f'Erro ao consultar ou atualizar os dados: {error}')
                await context.bot.send_message(chat_id=chat_id, text=f'Erro ao consultar ou atualizar os dados: {error}')
            
            # await asyncio.sleep(40)  # Espera antes de verificar novamente
    except Exception as error:
        print(f'Erro ao conectar ao banco de dados: {error}')
        await context.bot.send_message(chat_id=chat_id, text=f'Erro ao conectar ao banco de dados: {error}')
    
    finally:
        if cursor:
            cursor.close()
        if conexao:
            conexao.close()
            print(f'Conexão encerrada para {chat_nome}.')

async def disparar(update: Update, context: ContextTypes.DEFAULT_TYPE):

    chat_id = update.message.chat_id

    # Verifique se o chat é um grupo ou uma conversa privada
    if update.message.chat.type in ['group', 'supergroup']:
        chat_nome = update.message.chat.title
    else:
        chat_nome = update.message.chat.first_name or update.message.chat.username or "Usuário"

    if chat_id not in active_groups:
        active_groups[chat_id] = asyncio.create_task(monitorar_grupo(chat_id, chat_nome, context))
        await update.message.reply_text('Monitoramento iniciado!')
    else:
        await update.message.reply_text('Já estou monitorando este grupo!')

async def sim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(''' ... ''')

async def nao(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Obrigado! Quando precisar, é só chamar')

def main():
    conexao_api = Application.builder().token("7438639096:AAFXWfT_FcbEGo-DllfS7FroPRNVluV2_cQ").build()

    conexao_api.add_handler(CommandHandler('ola', iniciar))
    conexao_api.add_handler(CommandHandler('info', info))
    conexao_api.add_handler(CommandHandler('disparar', disparar))
    conexao_api.add_handler(CommandHandler('SIM', sim))
    conexao_api.add_handler(CommandHandler('NEGATIVO', nao))
    conexao_api.add_handler(CommandHandler('Voltar', voltar))

    conexao_api.run_polling()

if __name__ == '__main__':
    main()



