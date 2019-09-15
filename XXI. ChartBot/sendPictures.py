# -*- coding: utf-8 -*-
"""
Created on Wed Aug 28 15:25:52 2019

@author: Woojin
"""


from telegram.ext import Updater, CommandHandler
import requests
import re
import urllib2

api_key = '969501539:AAE9y0VBLFVF-O2DGXwPNcIUMiirE17lBgg'
contents = requests.get('https://random.dog/woof.json').json()
image_url = contents['url']

def get_url():
    contents = requests.get('https://random.dog/woof.json').json()    
    url = contents['url']
    return url

#def bop(bot, update):
#    
#    url = get_url()
#    chat_id = update.message.chat_id
#    bot.send_photo(chat_id=chat_id, photo=url)

def bop(bot, update):
    urls = [get_url() for i in range(5)]
    for url in urls:
        ret = urllib2.urlopen(url)
        if ret.code == 200:
            chat_id = update.message.chat_id
            bot.send_media_group(chat_id=chat_id, photo=url)

updater = Updater(api_key)
dp = updater.dispatcher
dp.add_handler(CommandHandler('bop',bop))

updater.start_polling()
updater.idle()
    

