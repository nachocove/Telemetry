from django.utils.decorators import available_attrs
from django.views.decorators.cache import cache_control
from django.views.decorators.vary import vary_on_cookie
from functools import wraps

def nachotoken_required(view_func):
    @wraps(view_func, assigned=available_attrs(view_func))
    def _wrapped_view(request, *args, **kwargs):
        return view_func(request, *args, **kwargs)
        # if validate_session(request):
        #     return view_func(request, *args, **kwargs)
        # else:
        #     return HttpResponseRedirect(settings.LOGIN_URL)
    return _wrapped_view

def nacho_cache(view_func):
    """
    A convenient function where to adjust cache settings for all cached pages. If we later
    want to add 304 processing or server-side caching, just add it here.
    """
    @wraps(view_func, assigned=available_attrs(view_func))
    @cache_control(private=True, must_revalidate=True, proxy_revalidate=True, max_age=3600)
    @vary_on_cookie
    def _wrapped_view(request, *args, **kwargs):
        return view_func(request, *args, **kwargs)
    return _wrapped_view