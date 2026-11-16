using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.SignalR;
using KanbanBoard.Data;
using KanbanBoard.Hubs;

namespace KanbanBoard.Jobs
{
    // задачи, которые выполняет планировщик Hangfire
    public class BoardJobs
    {
        // через сколько напомнить о задаче в работе
        public static readonly TimeSpan ReminderDelay = TimeSpan.FromMinutes(1);
        // готовые задачи старше этого срока удаляются
        public static readonly TimeSpan DoneMaxAge = TimeSpan.FromDays(7);
        private readonly AppDbContext _context;
        private readonly IHubContext<BoardHub> _hub;
        private readonly ILogger<BoardJobs> _logger;
        public BoardJobs(AppDbContext context, IHubContext<BoardHub> hub, ILogger<BoardJobs> logger)
        {
            _context = context;
            _hub = hub;
            _logger = logger;
        }

        // разовая задача: запись в журнал о новой задаче
        // (fire-and-forget)
        public async Task CardCreated(int cardId)
        {
            var card = await _context.TaskCards.FindAsync(cardId);
            if (card == null) return;
            _logger.LogInformation("[Hangfire] Создана задача #{Id} «{Title}», автор: {Author}",
                card.Id, card.Title, card.Author);
        }

        // отложенная задача: напоминание, если задача всё ещё в работе
        // (delayed)
        public async Task RemindInProgress(int cardId)
        {
            var card = await _context.TaskCards.FindAsync(cardId);
            if (card == null || card.Status != "in_progress")
            {
                _logger.LogInformation("[Hangfire] Напоминание по задаче #{Id} не нужно", cardId);
                return;
            }

            var text = $"Напоминание: задача «{card.Title}» в работе уже больше {ReminderDelay.TotalMinutes} мин., исполнитель: {card.Assignee}";
            _logger.LogInformation("[Hangfire] {Text}", text);
            await _hub.Clients.All.SendAsync("Notify", text);
        }
        // периодическая задача: удаление старых готовых задач
        // (recurring)
        public async Task CleanupDone()
        {
            var border = DateTime.Now - DoneMaxAge;
            var oldCards = await _context.TaskCards
                .Where(c => c.Status == "done" && c.CreatedAt < border)
                .ToListAsync();

            _logger.LogInformation("[Hangfire] Очистка доски: найдено старых готовых задач: {Count}", oldCards.Count);
            if (oldCards.Count == 0) return;

            _context.TaskCards.RemoveRange(oldCards);
            await _context.SaveChangesAsync();

            // убрать удалённые карточки у всех, у кого открыта доска
            foreach (var card in oldCards)
                await _hub.Clients.All.SendAsync("CardDeleted", card.Id);
            await _hub.Clients.All.SendAsync("Notify", $"Планировщик удалил старые готовые задачи: {oldCards.Count} шт.");
        }
    }
}