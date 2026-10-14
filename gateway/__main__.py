"""Launch with python -m gateway; external exposure requires a TLS proxy."""

import os

import uvicorn


def main():
    uvicorn.run(
        "gateway.app:create_app",
        factory=True,
        host=os.environ.get("SAFETAG_GATEWAY_HOST", "127.0.0.1"),
        port=int(os.environ.get("SAFETAG_GATEWAY_PORT", "80")),
        access_log=False,
        log_level="critical",
    )


if __name__ == "__main__":
    main()
