using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using KanbanBoard.Data;
using KanbanBoard.Models;
using KanbanBoard.Jobs;
using Hangfire;

namespace KanbanBoard.Hubs
{
    public class BoardHub : Hub
    {
        // словарь статусов
        private static readonly Dictionary<string, string> StatusNames = new()
        {
            ["todo"] = "К выполнению",
            ["in_progress"] = "В работе",
            ["done"] = "Готово"
        };

        // количество вкладок с доской
        private static int _online; 
        
        private readonly AppDbContext _context;
        public BoardHub(AppDbContext context)
        {
            _context = context;
        }
        // подключение и отключение
        public override async Task OnConnectedAsync()
        {
            var cards = await _context.TaskCards.OrderBy(c => c.Id).ToListAsync();
            await Clients.Caller.SendAsync("Board", cards);              

            var online = Interlocked.Increment(ref _online);
            await Clients.All.SendAsync("Online", online);              
            await base.OnConnectedAsync();
        }
        public override async Task OnDisconnectedAsync(Exception? exception)
        {
            var online = Interlocked.Decrement(ref _online);
            await Clients.All.SendAsync("Online", online);              
            await base.OnDisconnectedAsync(exception);
        }
        
        // методы клиента
        
        // добавить карточку
        public async Task AddCard(string title, string author)
        {
            title = (title ?? "").Trim();
            if (title.Length == 0 || title.Length > 100) return;

            var card = new TaskCard { Title = title, Author = CleanName(author), Status = "todo", CreatedAt = DateTime.Now };
            _context.TaskCards.Add(card);
            await _context.SaveChangesAsync();

            await Clients.All.SendAsync("CardAdded", card); 
            await Clients.Others.SendAsync("Notify", $"Новая задача «{card.Title}», автор: {card.Author}");
            BackgroundJob.Enqueue<BoardJobs>(job => job.CardCreated(card.Id));
        }

        // переместить карточку в колонку
        public async Task MoveCard(int id, string status, string author)
        {
            if (!StatusNames.ContainsKey(status)) return;

            var card = await _context.TaskCards.FindAsync(id);
            if (card == null || card.Status == status) return;
            if (card.Status == "done") return; 

            var name = CleanName(author);  
            var oldStatus = card.Status;
            card.Status = status;
            if (status == "in_progress") card.Assignee = name;
            else if (status == "todo") card.Assignee = null;
            else if (status == "done" && card.Assignee == null) card.Assignee = name;
            await _context.SaveChangesAsync();
            await Clients.All.SendAsync("CardMoved", id, status, card.Assignee);

            var text = status == "in_progress"
                ? $"Задача «{card.Title}» взята в работу, исполнитель: {name}"
                : $"Задача «{card.Title}» сменила статус с {StatusNames[oldStatus]} на {StatusNames[status]} ({name})";
            await Clients.Others.SendAsync("Notify", text);
            if (status == "in_progress")
                BackgroundJob.Schedule<BoardJobs>(job => job.RemindInProgress(id), BoardJobs.ReminderDelay);
        }
        
        // удалить карточку
        public async Task DeleteCard(int id, string author)
        {
            var card = await _context.TaskCards.FindAsync(id);
            if (card == null) return;

            _context.TaskCards.Remove(card);
            await _context.SaveChangesAsync();

            await Clients.All.SendAsync("CardDeleted", id);
            await Clients.Others.SendAsync("Notify", $"Задача «{card.Title}» удалена ({CleanName(author)})");
        }
        
        private static string CleanName(string name)
        {
            name = (name ?? "").Trim();
            if (name.Length == 0) return "Аноним";
            if (name.Length > 30) name = name.Substring(0, 30);
            return name;
        }
    }
}
