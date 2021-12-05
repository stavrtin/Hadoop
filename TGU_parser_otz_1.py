# Импортируем библиотеки
from pprint import  pprint
from lxml import html
import requests
import pandas as pd # Библиотека для работы с таблицами
import nltk # Библиотека для работы с текстами
from nltk.tokenize import word_tokenize, sent_tokenize # токенайзер - разбивает непрерывный текст на токены(слова)
from nltk.stem.snowball import SnowballStemmer # стеммер - преобразует тексты к начальной форме
from nltk.corpus import stopwords # список незначимых слов - я, ты, он, она, там и тд
import string
import pymorphy2
from pymorphy2 import MorphAnalyzer

from dostoevsky.tokenization import RegexTokenizer
from dostoevsky.models import FastTextSocialNetworkModel





url_start = 'https://tabiturient.ru/sliv/n/?'

headers = {'User Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 YaBrowser/21.9.0.1044 Yowser/2.5 Safari/537.36'}
start = 14
end = 20

all_views = []
for item in range(start,end):
    url = url_start + str(item)
    response = requests.get(url, headers=headers)
    dom = html.fromstring(response.text)


    def get_opinion(dom, count):
        view = {}
        try: think_count = dom.xpath("//h1[@class='font4 fontshadow1']/text()")
        except: think_count = None
        try: think_text = dom.xpath("//div[contains(@style,'text-align')]/text()")
        except: think_text = None
        like = "//b[@id='likeval" + str(item)+ "']/text()"
        try: think_like = dom.xpath(like)
        except: think_like = None
        try: think_looks = dom.xpath("//td[@class='p20']/table[@cellspacing='0']/tbody/tr/td/span[@class='font2']/text()")[0]
        except: think_count = None
        try: think_date = dom.xpath("//td[@class='p20']/table[@cellspacing='0']/tbody/tr/td/span[@class='font2']/text()")[1]
        except: think_date = None
        try: think_univer = dom.xpath("//td[@class='w100mobile']/span[@class='font2']/b/text()")
        except: think_univer = None
        try: think_smile = dom.xpath("//img[contains(@src,'https://tabiturient.ru/img/sm')]")[0].values()
        except: think_smile = None

        view['id'] = count
        view['count'] = think_count
        view['date'] = think_date
        view['text'] = think_text
        view['like'] = think_like
        view['looks'] = think_looks
        view['univer'] = think_univer

        if think_smile and think_smile[0].find('smile1') != -1:
            view['smile'] = 'Positive'
        elif think_smile and think_smile[0].find('smile2') != -1:
            view['smile'] = 'Negative'
        else: view['smile'] = 'Neutral'

        return view

    all_views.append(get_opinion(dom, item))

# pprint(all_views)
df = pd.DataFrame(all_views)

#  ВОСПОЛЬЗУЕМСЯ ТЕМАТИЧЕСКИМ МОДЕЛИРОВАНИЕМ для вычисления наиболее часто встречающихся требований работодателя (в вакансиях)
# Необходимые импорты и подгрузки



nltk.download('all') # Подгрузка необходимых данных для работы работы nltk
nltk.download('punkt')
nltk.download('stopwords')

morph_analyzer = pymorphy2.MorphAnalyzer() # Создаем объект стеммера
ru_stop_words = stopwords.words('russian')  # Подгружаем список стоп слов из модуля stopwords библиотеки nltk
# ru_stop_words = ru_stop_words + ['готовность', 'работать', 'час', 'неделя', '40', '30']

punctuations = list(string.punctuation)
punctuations.extend(['•', '—', '–', '«', '»', "'", '``', '“', '”', '.', '’', '·', '●'])


# готовность работать 40
def text_processing(text, morph_analyzer, stop_words, punct):
    """принимает на вход предложение и возвращает леммы токенов предложения, фильтруя по стоп словам и удаляя знаки пунктуации"""
    words = word_tokenize(text.lower()) # С помощью токенизации разбиваем текст на токены
    words = [word for word in words if word not in stop_words] # Удаляем стопслова
    words = [word for word in words if word not in punct] # Удаляем знаки пунктуации
    words = [morph_analyzer.parse(word)[0].normalized.word for word in words] # лемматизируем каждое слово
    words = ' '.join(words)

    return words

#  перегнали List в str
df['_text_tok'] = df['text'].apply(lambda x: (' '.join(x)))
# генерируем колонку lemmatized_text
df['_lemmatized'] = df['_text_tok'].apply(lambda x: text_processing(x, morph_analyzer, ru_stop_words, punctuations)) # с помощью метода apply прогоняем значения колонки text через нашу функцию обработки text_processing
df.head()



tokenizer = RegexTokenizer()
# tokens = tokenizer.split('всё очень плохо')  # [('всё', None), ('очень', None), ('плохо', None)]
model = FastTextSocialNetworkModel(tokenizer=tokenizer)
# model = FastTextSocialNetworkModel(tokenizer=tokenizer)

overview = df['_lemmatized']

results = model.predict(overview, k=3)

results = pd.DataFrame(results)
results.head()

print(results)