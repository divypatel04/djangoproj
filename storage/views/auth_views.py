"""Authentication views for the storage application."""

from django.shortcuts import render, redirect
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.models import User
from django.contrib import messages

def user_register(request):
    """Register a new user."""
    if request.method == "POST":
        username = request.POST["username"]
        password = request.POST["password"]

        if User.objects.filter(username=username).exists():
            messages.error(request, "Username already taken")
        else:
            user = User.objects.create_user(username=username, password=password)
            login(request, user)
            return redirect("file_list")

    return render(request, "storage/register.html")

def user_login(request):
    """Handle user login."""
    if request.method == "POST":
        username = request.POST["username"]
        password = request.POST["password"]
        user = authenticate(request, username=username, password=password)

        if user:
            login(request, user)
            return redirect("file_list")
        else:
            messages.error(request, "Invalid credentials")

    return render(request, "storage/login.html")

def user_logout(request):
    """Handle user logout."""
    logout(request)
    return redirect("index")
