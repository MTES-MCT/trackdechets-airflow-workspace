from peewee import (
    PostgresqlDatabase,
    Model,
    CharField,
    IntegerField,
    DateField,
    DateTimeField,
    BooleanField,
)
from airflow.models import Connection

airflow_con = Connection.get_connection_from_secrets("td_datawarehouse")
sql_uri = airflow_con.get_uri().replace("postgres", "postgresql")

db = PostgresqlDatabase(sql_uri)


class StockEtablissement(Model):
    class Meta:
        database = db
        table_name = "stock_etablissement"
        schema = "raw_zone_insee"
        primary_key = False

    siren = CharField()
    nic = CharField()
    siret = CharField(unique=True, primary_key=True)
    statutDiffusionEtablissement = CharField(null=True)
    dateCreationEtablissement = DateField(null=True)
    trancheEffectifsEtablissement = CharField(null=True)
    anneeEffectifsEtablissement = CharField(null=True)
    activitePrincipaleRegistreMetiersEtablissement = CharField(null=True)
    dateDernierTraitementEtablissement = DateTimeField(null=True)
    etablissementSiege = BooleanField(null=True)
    nombrePeriodesEtablissement = IntegerField(null=True)
    complementAdresseEtablissement = CharField(null=True)
    numeroVoieEtablissement = CharField(null=True)
    indiceRepetitionEtablissement = CharField(null=True)
    dernierNumeroVoieEtablissement = CharField(null=True)
    indiceRepetitionDernierNumeroVoieEtablissement = CharField(null=True)
    typeVoieEtablissement = CharField(null=True)
    libelleVoieEtablissement = CharField(null=True)
    codePostalEtablissement = CharField(null=True)
    libelleCommuneEtablissement = CharField(null=True)
    libelleCommuneEtrangerEtablissement = CharField(null=True)
    distributionSpecialeEtablissement = CharField(null=True)
    codeCommuneEtablissement = CharField(null=True)
    codeCedexEtablissement = CharField(null=True)
    libelleCedexEtablissement = CharField(null=True)
    codePaysEtrangerEtablissement = CharField(null=True)
    libellePaysEtrangerEtablissement = CharField(null=True)
    identifiantAdresseEtablissement = CharField(null=True)
    coordonneeLambertAbscisseEtablissement = CharField(null=True)
    coordonneeLambertOrdonneeEtablissement = CharField(null=True)
    complementAdresse2Etablissement = CharField(null=True)
    numeroVoie2Etablissement = CharField(null=True)
    indiceRepetition2Etablissement = CharField(null=True)
    typeVoie2Etablissement = CharField(null=True)
    libelleVoie2Etablissement = CharField(null=True)
    codePostal2Etablissement = CharField(null=True)
    libelleCommune2Etablissement = CharField(null=True)
    libelleCommuneEtranger2Etablissement = CharField(null=True)
    distributionSpeciale2Etablissement = CharField(null=True)
    codeCommune2Etablissement = CharField(null=True)
    codeCedex2Etablissement = CharField(null=True)
    libelleCedex2Etablissement = CharField(null=True)
    codePaysEtranger2Etablissement = CharField(null=True)
    libellePaysEtranger2Etablissement = CharField(null=True)
    dateDebut = DateField(null=True)
    etatAdministratifEtablissement = CharField(null=True)
    enseigne1Etablissement = CharField(null=True)
    enseigne2Etablissement = CharField(null=True)
    enseigne3Etablissement = CharField(null=True)
    denominationUsuelleEtablissement = CharField(null=True)
    activitePrincipaleEtablissement = CharField(null=True)
    nomenclatureActivitePrincipaleEtablissement = CharField(null=True)
    caractereEmployeurEtablissement = CharField(null=True)
