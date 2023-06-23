import random

from django.core.management.base import BaseCommand
from faker import Faker

from movies.models import Filmwork, Genre, Person, GenreFilmwork, PersonFilmwork


class Command(BaseCommand):
    help = 'Populates database with 10_000 film works.'

    def handle(self, *args, **options):
        fake = Faker()
        genres = Genre.objects.bulk_create([Genre(name=fake.word(), description=fake.text()) for _ in range(10)])
        persons = Person.objects.bulk_create([Person(full_name=fake.name()) for _ in range(1000)])
        for i in range(10_000):
            film_work = Filmwork(
                    title=fake.sentence(nb_words=3),
                    description=fake.sentence(nb_words=20),
                    creation_date=fake.date(),
                    rating=fake.random_digit(),
                    type=fake.random_element(elements=['movie', 'tv_show']),
            )
            film_work.save()
            for _ in range(2):
                film_work.genres.add(genres[random.randint(0, len(genres)-1)], through_defaults={})
            for _ in range(5):
                film_work.persons.add(
                    persons[random.randint(0, len(persons)-1)],
                    through_defaults={'role': fake.random_element(elements=['actor', 'director', 'writer'])},
                )
            self.stdout.write(self.style.SUCCESS(f'{i+1} of 10000'))
