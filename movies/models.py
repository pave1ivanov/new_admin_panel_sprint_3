import uuid

from django.db import models
from django.utils.translation import gettext_lazy as _
from django.core.validators import MinValueValidator, MaxValueValidator


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)

    def __str__(self):
        return self.name

    class Meta:
        # Таблицы в БД были созданы не в стандартной схеме - явно указываем.
        db_table = 'content\".\"genre'
        verbose_name = _('genre')
        verbose_name_plural = _('genres')


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full name'), max_length=255)

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = 'content\".\"person'
        verbose_name = _('person')
        verbose_name_plural = _('persons')


class Filmwork(UUIDMixin, TimeStampedMixin):
    class Type(models.TextChoices):
        movie = 'movie', _('movie')
        tv_show = 'tv_show', _('tv show')

    title = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateField(_('creation date'))
    rating = models.FloatField(_('rating'), blank=True, validators=[MinValueValidator(0),
                                                                    MaxValueValidator(100)])
    type = models.CharField(_('type'), choices=Type.choices)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    persons = models.ManyToManyField(Person, through='PersonFilmwork')

    def __str__(self):
        return self.title

    class Meta:
        db_table = 'content\".\"film_work'
        verbose_name = _('film work')
        verbose_name_plural = _('film works')
        indexes = [
            models.Index(fields=['creation_date'], name='film_work_creation_date_idx')
        ]


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created = models.DateField(auto_now_add=True)

    class Meta:
        db_table = 'content\".\"genre_film_work'
        verbose_name = _('film work genre')
        verbose_name_plural = _('film work genres')
        constraints = [
            models.UniqueConstraint(fields=['film_work', 'genre'], name='film_work_genre_idx'),
        ]


class PersonFilmwork(UUIDMixin):
    class Role(models.TextChoices):
        actor = 'actor', _('actor')
        director = 'director', _('director')
        writer = 'writer', _('writer')

    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.CharField(_('role'), choices=Role.choices, null=True, max_length=255)
    created = models.DateField(auto_now_add=True)

    class Meta:
        db_table = 'content\".\"person_film_work'
        verbose_name = _('role in film work')
        verbose_name_plural = _('roles in film work')
        constraints = [
            models.UniqueConstraint(fields=['film_work', 'person', 'role'], name='film_work_person_idx'),
        ]
