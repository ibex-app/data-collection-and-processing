from itertools import chain
import re

def get_declensions_ka(word):
    declensions = [word]
    is_private = word in ['აბხაზეთი', 'ოსეთი'] #TODO add private words
    add_plural = ~is_private
    # consonant-ended root
    if word[-1] in ['ი']:
        root = word[:-1]
        if not is_private and word[-2:-1] in ['ლ', 'მ', 'ნ', 'რ'] and word[-3:-2] in ['ა', 'ე', 'ო']: # 'გოდორი', 'კოკორი', 'მაწონი', 'ნიორი'...
            root_ = word[:-3] + word[-2:-1]
        else:
            root_ = root
        private_prefix = 'ი' if is_private else ''
        #singular
        declensions += [
            root + 'ივით',
            root + 'მა', 
            word + 'სავით', root + 'თან',  root + 'ზე',  root + 'ში',
            root_ + "ს", root_ + 'ის', root_ + 'ისთვის', root_ + 'ისგან', root_ + 'ისკენ', #root_ + 'ისებრ', 
            root_ + 'ით', root_ + 'ულად'
        ]
        #plural
        if not is_private:
            declensions += [
                root + 'ებ' + 'ივით',
                root + 'ებ' + 'მა', 
                root_ + 'ები' + 'სავით', root + 'ებ' + 'თან',  root + 'ებ' + 'ზე',  root + 'ებ' + 'ში',
                root_ + 'ებ' + 'ის', root_ + 'ებ' + 'ისთვის', root_ + 'ებ' + 'ისგან', root_ + 'ებ' + 'ისკენ', #root_ + 'ებ' + 'ისებრ', 
                root_ + 'ებ'+ 'ით'
            ]
    # vowel-ended root
    if word[-1] in ['ა', 'ე', 'ო', 'უ']:
        root = word
        root_ = root[:-1]
        declensions += [
            root + 'მ', 
            root + 'ს',
            root + 'სავით', root + 'სთან',  root + 'ზე',  root + 'ში',
            root_ + 'ის', root_ + 'ისთვის', root_ + 'ისგან', root_ + 'ისკენ', #root_ + 'ისებრ', root_ + 'ისთანავე',
            root_ + 'ით', root_ + 'ულად'
        ]
        #plural needs to be added

    return declensions

# ending with consonants

# For the words containing ['ə', 'e', 'i']  ['i',    'in',  'ə',  'də', 'dən']   as the last vowel in the word 
# For the words containing ['ö', 'ü' ]      ['ü',    'ün',  'ə',  'də', 'dən']
# For the words containing ['a', 'ı' ]      ['ı',    'ın',  'a',  'da', 'dan']
# For the words containing ['o', 'u' ]      ['u',    'un',  'a',  'da', 'dan']
# For the words containing ['ə', 'e', 'i']  ['ni',   'nin', 'yə', 'də', 'dən']
# For the words containing ['ö', 'ü' ]      ['nü',   'nün', 'yə', 'də', 'dən']
# For the words containing ['a', 'ı' ]      ['nı',   'nın', 'ya', 'da', 'dan']
# For the words containing ['o', 'u' ]      ['nu',   'nun', 'ya', 'da', 'dan']
# ending with vowels

# ending with consonants

# For the words containing ['ə', 'e', 'i']  ['i',    'in',  'ə',  'də', 'dən']   as the last vowel in the word 
# For the words containing ['ö', 'ü' ]      ['ü',    'ün',  'ə',  'də', 'dən']
# For the words containing ['a', 'ı' ]      ['ı',    'ın',  'a',  'da', 'dan']
# For the words containing ['o', 'u' ]      ['u',    'un',  'a',  'da', 'dan']
# For the words containing ['ə', 'e', 'i']  ['ni',   'nin', 'yə', 'də', 'dən']
# For the words containing ['ö', 'ü' ]      ['nü',   'nün', 'yə', 'də', 'dən']
# For the words containing ['a', 'ı' ]      ['nı',   'nın', 'ya', 'da', 'dan']
# For the words containing ['o', 'u' ]      ['nu',   'nun', 'ya', 'da', 'dan']
# ending with vowels

def get_declensions_az(word):
    is_phrase = ' ' in word
    declensions = []
    vowels = ['i','ü','e','ö','ə','a','o','u','ı']
    last_char = word[-1:]
    
    last_vowel = ''

    for char in [word[len(word)-i-1:len(word)-i] for i in range(0, len(word))]:
        if char in vowels:
            last_vowel = char
            break

    if last_vowel == '':
        # raise ValueError('last vowel not found')
        print('last_vowel not found for', word)
        return declensions

    if last_char == 'q':    
        # does not this rule works here?
        root_ = word[:-1] + 'ğ'
    elif last_char == 'k':
        root_ = word[:-1] + 'y'
    else:
        root_ = word
# nin ne nde nedn
    # print("is_phrase", is_phrase, ~is_phrase)
    # print("last_vowel", last_vowel, last_char in vowels)
    if last_char in vowels and not is_phrase:
        if last_vowel in ['ə', 'e', 'i']:
            declensions = ['ni', 'nin', 'yə', 'də', 'dən']
        if last_vowel in ['ö', 'ü' ]:
            declensions = ['nü', 'nün', 'yə', 'də', 'dən']
        if last_vowel in ['a', 'ı' ]:
            declensions = ['nı', 'nın', 'ya', 'da', 'dan']
        if last_vowel in ['o', 'u' ]:
            # Sülhquruculuğu
            #where this additional N-s come from?
            declensions = ['nu', 'nun', 'ya', 'da', 'dan']
    elif last_char in vowels and is_phrase:
        if last_vowel in ['ə', 'e', 'i']:
            declensions = ['ni', 'nin', 'nə', 'ndə', 'ndən']
        if last_vowel in ['ö', 'ü' ]:
            declensions = ['nü', 'nün', 'nə', 'ndə', 'ndən']
        if last_vowel in ['a', 'ı' ]:
            declensions = ['nı', 'nın', 'na', 'nda', 'ndan']
        if last_vowel in ['o', 'u' ]:
            declensions = ['nu', 'nun', 'na', 'nda', 'ndan']
    else:
        if last_vowel in ['ə', 'e', 'i']:
            declensions = ['i', 'in', 'ə', 'də', 'dən']
        if last_vowel in ['ö', 'ü' ]:
            declensions = ['ü', 'ün', 'ə', 'də', 'dən']
        if last_vowel in ['a', 'ı' ]:
            declensions = ['ı', 'ın', 'a', 'da', 'dan']
        if last_vowel in ['o', 'u' ]:
            declensions = ['u', 'un', 'a', 'da', 'dan']
    
    # return {
    #     "Nom."  : word, 
    #     "Acc"   : root_ + declensions[0], 
    #     "Gen"   : root_ + declensions[1], 
    #     "Dat"   : root_ + declensions[2], 
    #     "Loc"   : word  + declensions[3], 
    #     "Abl:"  : word  + declensions[4]
    # }
    return [
        word, 
        root_ + declensions[0], 
        root_ + declensions[1], 
        root_ + declensions[2], 
        word  + declensions[3], 
        word  + declensions[4]            
    ]

def get_declensions_hy(word):
    declensions = []
    vowels = ["ի", "ու", "է", "ը", "ե", "ո", "ա"]

    pattern_vowels = re.compile(r'ի|ու|է|ը|ե|ո|ա')
    syllab_count =  pattern_vowels.findall(word)

    if word[-1:] in vowels or word[-2:] == "ու":
    # ending with vowels
        # For the words ending with 'ի' ->  'ւոյ' + 'ւով' || 'ի', 'ւոյ' + 'ւոջ','ւոջէ','եաւ'
        root_ = word[:-1]
        if word[-1:] == 'ի':
            if 1:
                declensions = ['ի', 'ւոյ'] + ['ւով']
            else:
                declensions = ['ի', 'ւոյ'] + ['ւոջ','ւոջէ','եաւ']
        # other cases?
    else:
    # ending with consonants
        print('consonants')
        root_ = word ##+ "լ"
        if 1:
            declensions = ['', 'ի', 'է', 'աւ']
        if word[-3:] in ["եայ", "եայ", "եից"] or word[-4:] in ["եայք"]:
            declensions = ['', 'ի', 'է', 'իւ']
        if 3:
            if 1:
                declensions = ['', 'ու', 'է']
            else:
                declensions = ['', 'ու', 'է'] + ['ուէ']
        if 4:
            if 1:
                declensions = ['', 'ոյ', 'ով']
            else:
                declensions = ['', 'ոյ', 'ով'] + []
        if 5:
            declensions = ['', 'այ', 'աւ']

    return [ root_ + a for a in declensions]


def get_declensions_hy(word):
    declensions = []
    vowels = ["ի", "ու", "է", "ը", "ե", "ո", "ա"]

    pattern_vowels = re.compile(r'ի|ու|է|ը|ե|ո|ա')
    syllab_count =  pattern_vowels.findall(word)
    
    # ան
    if word[-3:] in ['ում', 'ուն', ] or (syllab_count == 1 and word[-1:] == 'ն' ): # in word[-1:] in ['ն', 'կ', 'ռ', 'զ'] 
        pass
    elif word[-1:] == 'ի':
        root_ = word[:-1]
        if 1:
            declensions = ['ի', 'ւոյ'] + ['ւով']
        else:
            declensions = ['ի', 'ւոյ'] + ['ւոջ','ւոջէ','եաւ']
    elif word[-3:] in ["եայ", "եայ", "եից"] or word[-4:] in ["եայք"]:
        root_ = word
        declensions = ['', 'ի', 'է', 'իւ']
    
    return [ root_ + a for a in declensions]

def get_declensions_hy_naive(word):
    return [word, word + 'ը', word + 'ի', word + 'ին', word + 'ից', word + 'ով', word + 'ում' ]

def get_declensions(words, lang):
    if not lang in ['ka', 'az', 'hy']:
        # raise Exception("lang % not supported"%lang) 
        return words
    if type(words) != list:
        raise Exception("First position parameter must be list") 

    func = get_declensions_ka if lang == 'ka' else get_declensions_az if lang == 'az' else get_declensions_hy_naive
    return list(chain.from_iterable([func(term) for term in words]))

