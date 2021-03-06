# add index in table 'Person'

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='filmwork',
            name='creation_date',
            field=models.DateField(db_index=True, verbose_name='creation'),
        ),
        migrations.AlterField(
            model_name='genre',
            name='name',
            field=models.CharField(db_index=True, max_length=255, verbose_name='name'),
        ),
        migrations.AlterField(
            model_name='person',
            name='full_name',
            field=models.CharField(db_index=True, max_length=255, verbose_name='name'),
        ),
        migrations.AlterUniqueTogether(
            name='genrefilmwork',
            unique_together={('film_work', 'genre')},
        ),
        migrations.AlterUniqueTogether(
            name='personfilmwork',
            unique_together={('film_work', 'person')},
        ),
    ]
