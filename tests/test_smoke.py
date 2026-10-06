def test_imports():
    import gateway
    from gateway import Config, GatewayService

    assert gateway is not None
    assert Config is not None
    assert GatewayService is not None
