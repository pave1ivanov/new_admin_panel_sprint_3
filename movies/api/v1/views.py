from rest_framework.viewsets import ModelViewSet
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q

from movies.models import Filmwork, PersonFilmwork
from .serializers import FilmworkSerializer


class FilmworkViewSet(ModelViewSet):
    queryset = Filmwork.objects.prefetch_related(
        'genres', 'persons'
    ).annotate(
        actors=ArrayAgg('persons__full_name', filter=Q(personfilmwork__role=PersonFilmwork.Role.actor)),
        directors=ArrayAgg('persons__full_name', filter=Q(personfilmwork__role=PersonFilmwork.Role.director)),
        writers=ArrayAgg('persons__full_name', filter=Q(personfilmwork__role=PersonFilmwork.Role.writer)),
    ).all()

    serializer_class = FilmworkSerializer
    http_method_names = ['get', ]
