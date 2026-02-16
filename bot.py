# ============== ЗАПУСК БОТА ==============
async def main():
    """Главная функция запуска."""
    global application
    
    if BOT_TOKEN == "ВАШ_ТОКЕН_ЗДЕСЬ":
        print("\n" + "="*50)
        print("⚠️  ВНИМАНИЕ! Необходимо установить токен бота!")
        print("="*50)
        return
    
    print("🚀 Запуск ЛОР-Помощника...")
    print("📊 Версия: 5.0.3 (Исправленная для хостинга)")
    print("⏰ Часовой пояс: UTC (все времена в БД)")
    print("💾 Job store: SQLAlchemyJobStore (persistent)")
    print("🔄 Retry: 3 попытки")
    print("🚦 Rate limit: 30/сек глобально, 1/сек на пользователя")
    print("🛡️ Integrity check: каждый час")
    print("-" * 50)
    
    print("🔄 Отключаем webhook...")
    import requests
    try:
        response = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook")
        print(f"✅ Webhook отключен: {response.json()}")
    except Exception as e:
        print(f"⚠️ Ошибка при отключении webhook: {e}")
    
    # СОЗДАЕМ ПРИЛОЖЕНИЕ
    application = create_application()
    
    # ЗАПУСКАЕМ ПЛАНИРОВЩИК
    scheduler.start()
    
    # ВОССТАНАВЛИВАЕМ НАПОМИНАНИЯ
    await scheduler.restore_reminders()
    
    print("✅ Бот запущен и готов к работе!")
    print("📝 Логи пишутся в reminders.log")
    print("📡 Режим: Long Polling")
    print("💡 Отправьте /start в Telegram: @LorPomoshnikBot")
    print("⏎ Нажмите Ctrl+C для остановки")
    
    # ИСПРАВЛЕНИЕ: НЕ ИСПОЛЬЗУЕМ run_polling()!
    await application.initialize()
    await application.start()
    
    # Запускаем polling отдельно
    await application.updater.start_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )
    
    # Держим бота запущенным
    try:
        # Бесконечное ожидание
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Бот остановлен")
    finally:
        # Корректное завершение
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        if scheduler:
            scheduler.shutdown()
        reminder_logger.info("SHUTDOWN - Бот остановлен корректно")

# ============== ТОЧКА ВХОДА ==============
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
