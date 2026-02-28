"""
HTTP client for DICOMweb QIDO-RS and WADO-RS endpoints.

Supports three authentication modes:
    none    – open endpoints (test servers, Orthanc dev)
    basic   – HTTP Basic Auth (username + password)
    bearer  – Authorization: Bearer <token>
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

# Default connect + read timeouts (seconds)
_DEFAULT_TIMEOUT = (10, 60)


class DICOMwebClient:
    """Thin HTTP wrapper around a DICOMweb endpoint (QIDO-RS + WADO-RS)."""

    def __init__(
        self,
        base_url: str,
        auth_type: str = "none",
        username: str | None = None,
        password: str | None = None,
        token: str | None = None,
        timeout: tuple[int, int] = _DEFAULT_TIMEOUT,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/dicom+json"})

        auth_type = (auth_type or "none").lower()
        if auth_type == "basic":
            if not username or not password:
                raise ValueError("auth_type=basic requires username and password")
            self._session.auth = HTTPBasicAuth(username, password)
        elif auth_type == "bearer":
            if not token:
                raise ValueError("auth_type=bearer requires token")
            self._session.headers["Authorization"] = f"Bearer {token}"
        elif auth_type != "none":
            raise ValueError(f"Unsupported auth_type '{auth_type}'. Use: none, basic, bearer")

    # ------------------------------------------------------------------
    # QIDO-RS helpers
    # ------------------------------------------------------------------

    def _qido_get(self, path: str, params: dict[str, Any]) -> list[dict]:
        """Execute a QIDO-RS GET and return the parsed JSON list."""
        url = f"{self.base_url}{path}"
        logger.debug("QIDO-RS GET %s params=%s", url, params)
        resp = self._session.get(url, params=params, timeout=self.timeout)
        if resp.status_code == 204:
            # No content — valid empty response
            return []
        resp.raise_for_status()
        # Some servers return empty body instead of 204
        if not resp.content:
            return []
        return resp.json()

    def query_studies(
        self,
        study_date_range: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        QIDO-RS: GET /studies?StudyDate={range}&limit={n}&offset={n}

        Args:
            study_date_range: DICOMweb date range string, e.g. "20231201-20231215".
            limit:            Max records per page.
            offset:           Zero-based record offset for pagination.

        Returns:
            List of DICOM JSON objects (dicts keyed by 8-char hex tag strings).
        """
        params: dict[str, Any] = {
            "StudyDate": study_date_range,
            "limit": limit,
            "offset": offset,
        }
        return self._qido_get("/studies", params)

    def query_series(
        self,
        study_date_range: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """QIDO-RS: GET /series?StudyDate={range}&limit={n}&offset={n}"""
        params: dict[str, Any] = {
            "StudyDate": study_date_range,
            "limit": limit,
            "offset": offset,
        }
        return self._qido_get("/series", params)

    def query_instances(
        self,
        study_date_range: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """QIDO-RS: GET /instances?StudyDate={range}&limit={n}&offset={n}"""
        params: dict[str, Any] = {
            "StudyDate": study_date_range,
            "limit": limit,
            "offset": offset,
        }
        return self._qido_get("/instances", params)

    # ------------------------------------------------------------------
    # WADO-RS
    # ------------------------------------------------------------------

    def retrieve_instance(
        self,
        study_uid: str,
        series_uid: str,
        sop_uid: str,
    ) -> bytes:
        """
        WADO-RS: retrieve a raw DICOM file.

        GET {base_url}/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}
        Accept: multipart/related; type="application/dicom"

        Returns:
            Raw bytes of the .dcm file (multipart/related — first part extracted).
        """
        url = f"{self.base_url}/studies/{study_uid}/series/{series_uid}/instances/{sop_uid}"
        logger.debug("WADO-RS GET %s", url)
        resp = self._session.get(
            url,
            headers={"Accept": 'multipart/related; type="application/dicom"'},
            timeout=self.timeout,
        )
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        if "multipart/related" in content_type:
            return _extract_first_multipart_part(resp.content, content_type)
        # Some servers return raw DICOM directly
        return resp.content


# ---------------------------------------------------------------------------
# Multipart helpers
# ---------------------------------------------------------------------------


def _extract_first_multipart_part(body: bytes, content_type: str) -> bytes:
    """
    Extract the raw bytes of the first part from a MIME multipart body.

    The boundary is parsed from the Content-Type header, e.g.:
        multipart/related; type="application/dicom"; boundary="myboundary"
    """
    boundary = _parse_boundary(content_type)
    if not boundary:
        # Can't parse boundary — return whole body and hope for the best
        return body

    sep = f"--{boundary}".encode()
    parts = body.split(sep)
    # parts[0] is preamble (empty), parts[1] is first part, parts[-1] is "--\r\n"
    for part in parts[1:]:
        if part.strip() in (b"", b"--", b"--\r\n", b"--\n"):
            continue
        # Strip part headers (blank line separates headers from body)
        if b"\r\n\r\n" in part:
            return part.split(b"\r\n\r\n", 1)[1].rstrip(b"\r\n")
        if b"\n\n" in part:
            return part.split(b"\n\n", 1)[1].rstrip(b"\n")
        return part
    return body


def _parse_boundary(content_type: str) -> str | None:
    """Extract the multipart boundary from a Content-Type header value."""
    for segment in content_type.split(";"):
        segment = segment.strip()
        if segment.lower().startswith("boundary="):
            boundary = segment[len("boundary=") :].strip().strip('"')
            return boundary
    return None
