from telegram import Update
from telegram.ext import Application, MessageHandler, filters

TOKEN = "7384843477:AAFsitozSLRZvyFAuu_ZSEVSm1st_cnC0DA"

app = Application.builder().token(TOKEN).build()


async def handle_text(update: Update, context) -> None:
    chat_id = '@SportPrognoze2'
    text = update.message.text
    await context.bot.send_message(chat_id=chat_id, text=text)


text_handler = MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text)

app.add_handler(text_handler)

if __name__ == "__main__":
    app.run_polling()
