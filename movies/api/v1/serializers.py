from rest_framework.serializers import ModelSerializer, CharField, ListField

from movies.models import Filmwork, Genre


class GenreSerializer(ModelSerializer):
    class Meta:
        model = Genre
        fields = ('name', )


class FilmworkSerializer(ModelSerializer):
    genres = GenreSerializer(many=True, read_only=True)
    actors = ListField(child=CharField())
    directors = ListField(child=CharField())
    writers = ListField(child=CharField())

    class Meta:
        model = Filmwork
        fields = (
            'id',
            'title',
            'description',
            'creation_date',
            'rating',
            'type',
            'genres',
            'actors',
            'directors',
            'writers',
        )
