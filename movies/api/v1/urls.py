from rest_framework.routers import DefaultRouter

from .views import FilmworkViewSet


router = DefaultRouter()
router.register(r'movies', FilmworkViewSet, basename='movies')
urlpatterns = router.urls
