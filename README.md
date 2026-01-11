# anibridge-mal-provider

An [AniBridge](https://github.com/anibridge/anibridge) provider for [MyAnimeList](https://myanimelist.net/).

_This provider comes built-in with AniBridge, so you don't need to install it separately._

## Configuration

### `token` (`str`)

Your MyAnimeList API refresh token. You can generate one [here](https://anibridge.eliasbenb.dev?generate_token=mal).

### `client_id` (`str`, optional)

Your MyAnimeList API client ID. This option is for advanced users who want to use their own client ID. If not provided, a default client ID managed by the AniBridge team will be used.

```yaml
list_provider_config:
  mal:
    token: ...
    client_id: "b11a4e1ead0db8142268906b4bb676a4"
```
