# src/auditor/utils/stopwords.py
import functools

STOPWORDS_EN = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at", "be",
    "because", "been", "before", "being", "below", "between", "both", "but", "by", "can", "could", "did", "do", "does",
    "doing", "down", "during", "each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "her",
    "here", "hers", "him", "his", "how", "i", "if", "in", "into", "is", "it", "its", "itself", "just", "me", "more",
    "most", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "our", "ours",
    "ourselves", "out", "over", "own", "same", "she", "should", "so", "some", "such", "than", "that", "the", "their",
    "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those", "through", "to", "too", "under",
    "until", "up", "very", "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "with",
    "you", "your", "yours", "yourself", "yourselves", "for the", "of the", "our", "say", "0", "1", "2", "3", "4", "5",
    "6", "7", "8", "9", "+", "=", "*", "$", "%", "@", "!", "<", ">"
}

STOPWORDS_NL = {
    "aan", "aangaande", "aldus", "alhier", "alle", "allebei", "alleen", "alles", "als", "alsnog", "altijd", "althans",
    "anders", "ook", "behalve", "beiden", "ben", "bent", "bij", "bijna", "binnen", "boven", "buiten", "daarentegen",
    "daarheen", "daarom", "daarop", "daarvan", "dat", "de", "der", "deze", "die", "dit", "doch", "doen", "door", "dus",
    "echter", "een", "eens", "en", "enz", "er", "erg", "erdoor", "even", "eveneens", "evenwel", "gauw", "ge", "geen",
    "geleden", "gelijk", "gemogen", "geweest", "haar", "had", "hadden", "heb", "hebben", "heeft", "hem", "hen", "het",
    "hierbeneden", "hierboven", "hij", "hoe", "hoewel", "hun", "ik", "ieder", "iedere", "indien", "in", "inmiddels",
    "is", "je", "jij", "jou", "jouw", "jullie", "kan", "kon", "konden", "kunnen", "laat", "later", "liever", "maar",
    "mag", "men", "met", "mij", "mijn", "moet", "moeten", "na", "naar", "nadat", "naast", "net", "niet", "noch", "nog",
    "nu", "of", "omdat", "om", "omtrent", "onder", "ondertussen", "ons", "onze", "op", "over", "reeds", "slechts",
    "sinds", "sommige", "spoedig", "steeds", "te", "tegen", "toch", "toen", "tot", "tussen", "uit", "uiteindelijk",
    "van", "vanaf", "vanwege", "veel", "verder", "vervolgens", "via", "voor", "vooral", "voordat", "vroeg", "waarom",
    "wanneer", "want", "waren", "was", "wat", "weer", "weg", "wel", "welke", "wellicht", "wie", "wiens", "wier", "wil",
    "willen", "wordt", "worden", "zou", "zouden", "zullen", "zulk", "zulke", "zijn", "zo", "zodra", "zodat", "zonder",
    "btw", "incl", "inc", "wij", "nl", "pagina", "pag"
}

STOPWORDS_GUI = {
    "cookie", "cookies", "cookiebeleid", "begin", "eind", "start", "menu", "disclaimer", "voorwaarden", "klik", "via",
    "gingen", "nodig", "javascript", "komen", "privacyverklaring", "graag", "bekijk", "hét", "gaan", "prima", "kun",
    "komt", "hetzelfde", "gaat", "erg", "kortom", "gebruiken", "laatste", "inhoudsopgave", "blijven", "wellicht",
    "bezig", "willen", "published", "gezien", "nee", "goed", "true", "maakt", "maken", "maak", "downloads", "download",
    "algemene", "login", "inloggen", "wachtwoord", "vergeten", "plaats", "daarvan", "for", "your", "yours", "to"
}

def get_stopwords():
    return STOPWORDS_EN, STOPWORDS_NL, STOPWORDS_GUI

def combine_stopwords(func):
    """Decorator die een gecombineerde STOPWORDS-set aan de functie toevoegt."""
    @functools.wraps(func)
    def wrapper(self, text, stopwords=None, *args, **kwargs):
        # ✅ Combineer alle stopwoordenlijsten als ze niet zijn meegegeven
        if stopwords is None:
            stopwords = STOPWORDS_EN.union(STOPWORDS_NL).union(STOPWORDS_GUI)
        return func(self, text, stopwords, *args, **kwargs)
    return wrapper