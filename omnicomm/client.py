import time
import asyncio
from typing import Optional, Any, List

import httpx

import config
from .exceptions import OmnicommAuthError, OmnicommAPIError


class OmnicommClient:
    """
    Асинхронный клиент для Omnicomm API на базе httpx.AsyncClient.
    """
    def __init__(self, client: Optional[httpx.AsyncClient] = None):
        self.base_url = config.OMNICOMM_BASE_URL
        self.login = config.OMNICOMM_USERNAME
        self.password = config.OMNICOMM_PASSWORD

        self._token: Optional[str] = None
        self._token_expire_at: float = 0.0

        self._client = client or httpx.AsyncClient(timeout=config.REQUEST_TIMEOUT)
        self._lock = asyncio.Lock()

    async def _login(self) -> None:
        async with self._lock:
            if self._token and time.time() < self._token_expire_at:
                return

            url = f"{self.base_url}/auth/login?jwt=1"
            try:
                resp = await self._client.post(
                    url,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    },
                    json={"login": self.login, "password": self.password},
                )
            except httpx.RequestError as exc:
                raise OmnicommAuthError(f"Login request failed: {exc}") from exc

            if resp.status_code != 200:
                raise OmnicommAuthError(f"Login failed ({resp.status_code}): {resp.text}")

            try:
                body = resp.json()
            except ValueError as exc:
                raise OmnicommAuthError("Login returned invalid JSON") from exc

            token = body.get("jwt")  # API возвращает 'jwt', а не 'token'
            if not token:
                raise OmnicommAuthError(f"Login response missing token (jwt): {body}")

            self._token = token
            self._token_expire_at = time.time() + (55 * 60)

    async def _get_token(self) -> str:
        if not self._token or time.time() >= self._token_expire_at:
            await self._login()
        assert self._token is not None
        return self._token

    async def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        url = f"{self.base_url}{endpoint}"

        try:
            token = await self._get_token()
            headers = {"Authorization": f"JWT {token}", "Accept": "application/json"}
            resp = await self._client.request(method, url, headers=headers, **kwargs)
        except httpx.RequestError as exc:
            raise OmnicommAPIError(f"Request failed: {exc}") from exc

        if resp.status_code == 401:
            async with self._lock:
                await self._login()
                headers["Authorization"] = f"JWT {self._token}"

            try:
                resp = await self._client.request(method, url, headers=headers, **kwargs)
            except httpx.RequestError as exc:
                raise OmnicommAPIError(f"Request retry failed: {exc}") from exc

        if resp.status_code >= 400:
            raise OmnicommAPIError(f"HTTP {resp.status_code}: {resp.text}")

        try:
            return resp.json()
        except ValueError:
            return resp.text

    async def get_terminals(self) -> Any:
        return await self._request("GET", "/ls/api/v1/profile/terminals/list")

    async def get_terminal_profile(self, terminal_id: str) -> Any:
        return await self._request("GET", f"/ls/api/v1/profile/terminal/{terminal_id}")

    async def get_vehicles(self) -> Any:
        return await self._request("GET", "/ls/api/v1/profile/vehicles/list")

    async def get_vehicle_profile(self, vehicle_id: str) -> Any:
        return await self._request("GET", f"/ls/api/v1/profile/vehicle/{vehicle_id}")

    async def get_vehicle_state(self, vehicle_id: str) -> Any:
        return await self._request("GET", f"/ls/api/v1/vehicles/{vehicle_id}/state")

    async def get_rpm_report(self, vehicle_ids: List[int], from_datetime: int, to_datetime: int) -> Any:
        payload = {
            "vehicleIds": vehicle_ids,
            "fromDatetime": from_datetime,
            "toDatetime": to_datetime
        }
        return await self._request("POST", "/ls/api/v1/reports/rpms", json=payload)

    async def get_track_report(self, vehicle_id: str, time_begin: int, time_end: int) -> Any:
        """
        Получение трека ТС за указанный интервал времени.
        Args:
            vehicle_id: ID терминала или UUID
            time_begin: начало интервала (Unix timestamp в секундах)
            time_end: конец интервала (Unix timestamp в секундах)
        Returns:
            Словарь с треком: {"track": [{"date": ..., "latitude": ..., ...}]}
        """
        return await self._request("GET", f"/ls/api/v1/reports/track/{vehicle_id}?timeBegin={time_begin}&timeEnd={time_end}")

    async def aclose(self) -> None:
        await self._client.aclose()