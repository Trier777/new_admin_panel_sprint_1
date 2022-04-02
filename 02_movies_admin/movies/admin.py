from django.contrib import admin
from movies.models import Genre, Person, Filmwork, GenreFilmwork, PersonFilmWork


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.prefetch_related('person')


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    search_fields = ('name',)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name',)
    search_fields = ('full_name',)


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmWorkInline,)
    list_display = ('title', 'type', 'creation_date', 'rating',)
    list_filter = ('type', 'rating',)
    search_fields = ('title', 'description', 'id')
