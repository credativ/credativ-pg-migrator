import hashlib
import random
from credativ_pg_migrator.anonymization.registry import anonymization_registry

# In-memory integer mapping cache for consistent_integer_mask
_int_cache = {}

def get_faker(locale='de_DE'):
    try:
        from faker import Faker
        return Faker(locale)
    except ImportError:
        raise ImportError("The 'Faker' library is required for this anonymization method. Please install it using 'pip install faker'.")

def get_mimesis_address(locale='de'):
    try:
        from mimesis import Address
        from mimesis.locales import Locale
        loc = Locale.DE if locale == 'de' else Locale.EN
        return Address(locale=loc)
    except ImportError:
        raise ImportError("The 'mimesis' library is required for this anonymization method. Please install it using 'pip install mimesis'.")

@anonymization_registry.register('faker_name')
def faker_name(value, params):
    fake = get_faker('de_DE')
    return fake.name()

@anonymization_registry.register('faker_email')
def faker_email(value, params):
    fake = get_faker('de_DE')
    return fake.email()

@anonymization_registry.register('mimesis_address')
def mimesis_address(value, params):
    address = get_mimesis_address('de')
    part = params.get('part', 'full')
    
    if part == 'full':
        return f"{address.street_name()} {address.street_number()}, {address.postal_code()} {address.city()}"
    elif part == 'street_address':
        return address.address()
    elif part == 'street_name_house_number':
        return f"{address.street_name()} {address.street_number()}"
    elif part == 'street_name':
        return address.street_name()
    elif part == 'house_number':
        return address.street_number()
    elif part == 'postal_code':
        return address.postal_code()
    elif part == 'city':
        return address.city()
    return address.address()

@anonymization_registry.register('custom_iban')
def custom_iban(value, params):
    fake = get_faker('de_DE')
    return fake.iban()

@anonymization_registry.register('custom_german_bank')
def custom_german_bank(value, params):
    # Simplified mock for demonstration
    part = params.get('part', 'combined')
    blz = f"{random.randint(100,999)}50000"
    kto = f"{random.randint(1000000000, 9999999999)}"
    if part == 'blz':
        return blz
    elif part == 'kontonummer':
        return kto
    return f"BLZ: {blz}, KTO: {kto}"

@anonymization_registry.register('consistent_integer_mask')
def consistent_integer_mask(value, params):
    if value is None:
        return None
    domain = params.get('domain', 'default')
    cache_key = f"{domain}_{value}"
    
    if cache_key not in _int_cache:
        _int_cache[cache_key] = random.randint(1000000, 9999999)
    return _int_cache[cache_key]

@anonymization_registry.register('static_mask')
def static_mask(value, params):
    if not value:
        return value
    mask_char = params.get('mask_char', 'X')
    return mask_char * len(str(value))

@anonymization_registry.register('deterministic_hash_mask')
def deterministic_hash_mask(value, params):
    if value is None:
        return None
    salt = params.get('salt', '')
    out_type = params.get('out_type', 'string')
    
    hashed = hashlib.sha256(f"{value}{salt}".encode()).hexdigest()
    if out_type == 'int':
        return int(hashed[:8], 16)
    return hashed

@anonymization_registry.register('numeric_noise')
def numeric_noise(value, params):
    if value is None:
        return None
    try:
        val = float(value)
        ratio = float(params.get('ratio', 0.20))
        noise = val * ratio * random.uniform(-1, 1)
        res = val + noise
        if isinstance(value, int):
            return int(res)
        return res
    except (ValueError, TypeError):
        return value

@anonymization_registry.register('partial_mask')
def partial_mask(value, params):
    if not value:
        return value
    val_str = str(value)
    prefix_len = int(params.get('prefix_len', 0))
    suffix_len = int(params.get('suffix_len', 0))
    mask_str = params.get('mask_str', '***')
    
    if len(val_str) <= prefix_len + suffix_len:
        return mask_str
        
    return val_str[:prefix_len] + mask_str + val_str[len(val_str)-suffix_len:]

@anonymization_registry.register('postgres_anon_native')
def postgres_anon_native(value, params):
    func_name = params.get('func_name', 'anon.fake_city')
    args = params.get('args', '')
    pass_original = params.get('pass_original', False)
    
    if pass_original:
        if args:
            return f"__RAW_SQL__:{func_name}(%s, {args})"
        else:
            return f"__RAW_SQL__:{func_name}(%s)"
    else:
        if args:
            return f"__RAW_SQL__:{func_name}({args})"
        else:
            return f"__RAW_SQL__:{func_name}()"
