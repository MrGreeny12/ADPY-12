import csv
import re
import datetime
from pymongo import MongoClient


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        events = db['events']
        for row in reader:
            day, month = map(int, row['Дата'].split('.'))
            event = {
                'artist': row['Исполнитель'],
                'price': int(row['Цена']),
                'location': row['Место'],
                'date': datetime.datetime(year=2020, month=month, day=day)
            }
            var = events.insert_one(event).inserted_id
        print(f'Данные записаны в коллекцию events, базы {db}.')


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    sorted_price = db.event.find().sort('price')
    return [(event['artist'], f"{event['price']}\u20BD", event['location'], str(event['date']))
            for event in sorted_price]


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """
    regex = re.compile(f'.*{name}.*', re.IGNORECASE)
    search_name = db.event.find({'artist': regex}).sort('price')
    return [(event['artist'], f"{event['price']}\u20BD", event['location'], str(event['date']))
            for event in search_name]


def find_sorted_for_date(db):
    sorted_date = db['event'].find().sort('date')
    return [(event['artist'], f"{event['price']}\u20BD", event['location'], str(event['date']))
            for event in sorted_date]


if __name__ == '__main__':
    client = MongoClient()
    db = client['db']

    read_data('artists.csv', db)
    find_cheapest(db)
    find_by_name(db)
    find_sorted_for_date(db)
    client.close()