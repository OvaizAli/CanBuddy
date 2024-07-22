css = '''
<style>
.chat-message {
    padding: 1.5rem; border-radius: 0.5rem; margin-bottom: 1rem; display: flex
}
.chat-message.user {
    background-color: #2b313e;  /* Dark grey for user messages */
    color: #f0f0f0;  /* Very light grey text for user messages */
    padding: 10px;
    margin: 10px 0;
    border-radius: 8px;
}

.chat-message.bot {
    background-color: #475063;  /* Slightly lighter grey for bot messages */
    color: #f0f0f0;  /* Very light grey text for bot messages */
    padding: 10px;
    margin: 10px 0;
    border-radius: 8px;
}
.chat-message .avatar {
  width: 20%;
}
.chat-message .avatar img {
  max-width: 78px;
  max-height: 78px;
  border-radius: 50%;
  object-fit: cover;
}
.chat-message .message {
  width: 80%;
  padding: 0 1.5rem;
  color: #fff;
}
'''

bot_template = '''
<div class="chat-message bot">
    <div class="avatar">
        <img src="https://www.pngall.com/wp-content/uploads/5/Profile-Avatar-PNG.png">
    </div>
    <div class="message">{{MSG}}</div>
</div>
'''

user_template = '''
<div class="chat-message user">
    <div class="avatar">
        <img src="https://cdn.pixabay.com/photo/2023/07/03/04/41/robot-8103345_640.png">
    </div>    
    <div class="message">{{MSG}}</div>
</div>
'''

