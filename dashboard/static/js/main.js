// Tự động reload log ở trang Home mỗi 3 giây
if (window.location.pathname === '/' || window.location.pathname === '/home') {
    setInterval(function() {
      fetch('/api/logs')
        .then(response => response.json())
        .then(data => {
          const container = document.getElementById('logs-container');
          if (!container) return;
          container.innerHTML = '';
          data.forEach(log => {
            const row = `<tr>
                          <td>${log.id}</td>
                          <td>${log.topic}</td>
                          <td>${log.partition}</td>
                          <td>${log.offset}</td>
                          <td>${log.message}</td>
                          <td>${log.created_at}</td>
                        </tr>`;
            container.innerHTML += row;
          });
        });
    }, 1000);
  }
  