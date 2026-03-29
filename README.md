# anibridge-mal-provider

An [AniBridge](https://github.com/anibridge/anibridge) provider for [MyAnimeList](https://myanimelist.net/).

_This provider comes built-in with AniBridge, so you don't need to install it separately._

## Configuration

```yaml
list_provider_config:
  mal:
    token: ...
    # client_id: "b11a4e1ead0db8142268906b4bb676a4"
    # rate_limit: null
```

### `token`

`str` (required)

Your MyAnimeList API refresh token. You can generate one [here](https://anibridge.eliasbenb.dev?generate_token=mal).

### `client_id`

`str` (optional, default: `"b11a4e1ead0db8142268906b4bb676a4"`)

Your MyAnimeList API client ID. This option is for advanced users who want to use their own client ID. If not provided, a default client ID managed by the AniBridge team will be used.

### `rate_limit`

`int | None` (optional, default: `null`)

The maximum number of API requests per minute.

If unset or set to `null`, the provider will use a default global rate limit of 60 requests per minute. This global limit is shared across all MAL provider instances. If you override the rate limit, a local per-instance limiter is created instead.
