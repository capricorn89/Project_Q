# -*- coding: utf-8 -*-
"""
Created on Fri Jun  1 09:30:03 2018

@author: User
"""

import numpy as np
import pandas as pd
import torch
import torchvision
from torch import autograd, nn, optim
import torch.nn.functional as F
import matplotlib.pyplot as plt
import _pickle as cPickle
import torchvision.transforms as transforms

#%% Get CIFAR data
transform = transforms.Compose([transforms.ToTensor()])

cifar_train = torchvision.datasets.CIFAR100(root='./data', train=True, download=True, transform=transform)
cifar_test = torchvision.datasets.CIFAR100(root='./data', train=False, download=True, transform=transform)

trainloader = torch.utils.data.DataLoader(cifar_train, batch_size=1, shuffle=True)
testloader = torch.utils.data.DataLoader(cifar_test, batch_size=1, shuffle=False)

file = open("C:/Users/User/Desktop/업무/data/cifar-100-python/meta",'rb')
cifar_name = cPickle.load(file)
cifar_name = cifar_name['fine_label_names']
file.close()

#%% Visualizing random CIFAR traindata
def Visual_cifar(train):
    data = (train.train_data)
    label = (train.train_labels)
    ran = np.random.randint(len(data))

    plt.title('Label is {label}'.format(label=cifar_name[label[ran]]))
    plt.imshow(data[ran])
    
#%% Visualizing testdata and predicted value (run after learning)
def Visual_mnist_test(mode):  # network를 다 학습시킨 다음, testset에서 visualization. Feedforward는 'F', Convolutional은 'C', GoogleNet은 'G'
    ran = np.random.randint(len(cifar_test))
    img = testloader.dataset.test_data[ran]
    labels = testloader.dataset.test_labels[ran]
    if mode == 'F':
        inputs = img.type(torch.FloatTensor).view(1,28*28)
        out = net(inputs)
    elif mode == 'C':
        inputs = img.type(torch.FloatTensor).reshape(1,1,28,28)   
        out = conv(inputs)  
    elif mode == 'G':
        inputs = img.type(torch.FloatTensor).reshape(1,1,28,28)   
        out = goog(inputs)
    
    _, pred = out.max(1)
    
    plt.title('Prediction is {pred}, Label is {label}'.format(pred=pred.item(),label=labels.item()))
    plt.imshow(img)
    
#%% Basic Feedforward Net (cifar100) ----- 82.5%, 2번 돌리면 72%
class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()  # super는 다중 상속할 때 쓰는 함수. nn.Module이 object를 상속받고 있으므로 super를 써준다.
        # 그냥 아래는 내 맘대로 만든 간단한 network
        self.l1 = nn.Linear(3*32*32,10)
        self.l2 = nn.Linear(1000,300)
        self.l3 = nn.Linear(300,100)
        self.soft = nn.Softmax(dim=1)
        
    def forward(self,x):
        x = F.relu(self.l1(x))
        x = F.relu(self.l2(x))
        x = self.l3(x)
        x = self.soft(x)
        return x

net = Net()
optimizer = optim.Adam(net.parameters(), lr = 0.001)  # optimizer 바꿔도 됨
loss_func = nn.CrossEntropyLoss()  # loss function 바꿔도 

running_loss = 0
    

for epoch, data in enumerate(trainloader):
    inputs, labels = data
    inputs = inputs.view(1,3*32*32)  # net에 넣기 전에 데이터 모양 맞춰주기
    optimizer.zero_grad()
    
    out = net(inputs)
    _, pred = out.max(1)
  
    loss = loss_func(out, labels)
    loss.backward()  # back propagation
    optimizer.step()        
    
    running_loss += loss
    
    if (epoch+1)%1000 == 0:
        print('epoch : %d, loss : %.3f [target : %d, pred : %d] ' %
                  (epoch + 1, running_loss/1000, labels.numpy().item(), pred.numpy().item()))
        running_loss = 0  

#%% Convolutional Network (mnist)  98.92%
class ConvNet(nn.Module):
    def __init__(self):
        super(ConvNet, self).__init__()
        self.layer1 = nn.Sequential(  # 첫 번째 레이어는 아래의 network으로 구성
            nn.Conv2d(1, 16, kernel_size=5, stride=1, padding=2),  # (1,1,28,28) -> (1,16,28,28) / 16개 층이 stack
            nn.BatchNorm2d(16),  # (1,16,28,28) -> (1,16,28,28) / 각각의 batch마다 normalization 수행
            nn.ReLU(),  # (1,16,28,28) -> (1,16,28,28) / non-linearity
            nn.MaxPool2d(kernel_size=2, stride=2))  # (1,16,28,28) -> (1,16,14,14) / 2*2 kernel로 maxpool수행하므로 14로 size 축소
        self.layer2 = nn.Sequential(
            nn.Conv2d(16, 32, kernel_size=5, stride=1, padding=2),  # (1,16,14,14) -> (1,32,14,14) / 32개 층이 stack
            nn.BatchNorm2d(32),  # (1,32,14,14) -> (1,32,14,14)
            nn.ReLU(),  # (1,32,14,14) -> (1,32,14,14)
            nn.MaxPool2d(kernel_size=2, stride=2))   # (1,32,14,14) -> (1,32,7,7)
        self.fc = nn.Linear(7*7*32, 10)  # (1,32*7*7) -> (1,10)
        
    def forward(self, x):
        out = self.layer1(x)
        out = self.layer2(out)
        out = out.reshape(out.size(0), -1)  # (1,32,14,14) -> (1,32*7*7)
        out = self.fc(out)
        return out

conv = ConvNet()

loss_func = nn.CrossEntropyLoss()
optimizer = torch.optim.Adam(conv.parameters(), lr=0.001)

running_loss = 0
for i in range(2):
    for epoch, data in enumerate(trainloader):
        inputs, labels = data
        
        optimizer.zero_grad()
        
        out = conv(inputs)
        _, pred = out.max(1)
      
        loss = loss_func(out, labels)
        loss.backward()  # back propagation
        optimizer.step()        
        
        running_loss += loss
        
        if (epoch+1)%1000 == 0:
            print('epoch : %d, loss : %.3f [target : %d, pred : %d] ' %
                      (epoch + 1, running_loss/1000, labels.numpy().item(), pred.numpy().item()))
            running_loss = 0  
                        
#%% Google Net (mnist) 97.6%
""" 
# using cuda
use_cuda = torch.cuda.is_available()
if use_cuda:
    model = model.cuda()
"""

class Inception(nn.Module):  # Google net에 쓰이는 Inception들 클래스로 정의. nn.Module의 하위 클래스(상속)
    def __init__(self, in_planes, n1x1, n3x3red, n3x3, n5x5red, n5x5, pool_planes):  # 초기화하면서 변수 7개 받음()
        # in_planes는 
        super(Inception, self).__init__()
        # 1x1 conv branch
        self.b1 = nn.Sequential(
            nn.Conv2d(in_planes, n1x1, kernel_size=1),
            nn.BatchNorm2d(n1x1),
            nn.ReLU(True),
        )

        # 1x1 conv -> 3x3 conv branch
        self.b2 = nn.Sequential(
            nn.Conv2d(in_planes, n3x3red, kernel_size=1),
            nn.BatchNorm2d(n3x3red),
            nn.ReLU(True),
            nn.Conv2d(n3x3red, n3x3, kernel_size=3, padding=1),
            nn.BatchNorm2d(n3x3),
            nn.ReLU(True),
        )

        # 1x1 conv -> 5x5 conv branch
        self.b3 = nn.Sequential(
            nn.Conv2d(in_planes, n5x5red, kernel_size=1),
            nn.BatchNorm2d(n5x5red),
            nn.ReLU(True),
            nn.Conv2d(n5x5red, n5x5, kernel_size=3, padding=1),
            nn.BatchNorm2d(n5x5),
            nn.ReLU(True),
            nn.Conv2d(n5x5, n5x5, kernel_size=3, padding=1),
            nn.BatchNorm2d(n5x5),
            nn.ReLU(True),
        )

        # 3x3 pool -> 1x1 conv branch
        self.b4 = nn.Sequential(
            nn.MaxPool2d(3, stride=1, padding=1),
            nn.Conv2d(in_planes, pool_planes, kernel_size=1),
            nn.BatchNorm2d(pool_planes),
            nn.ReLU(True),
        )

    def forward(self, x):
        y1 = self.b1(x)
        y2 = self.b2(x)
        y3 = self.b3(x)
        y4 = self.b4(x)
        return torch.cat([y1,y2,y3,y4], 1)

class GoogLeNet(nn.Module):
    def __init__(self):
        super(GoogLeNet, self).__init__()
        self.pre_layers = nn.Sequential(
            nn.Conv2d(1, 192, kernel_size=3, padding=1),  #  (1,1,28,28) -> (1,192,28,28)
            nn.BatchNorm2d(192),  # (1,192,28,28) -> (1,192,28,28)
            nn.ReLU(True),  # (1,192,28,28) -> (1,192,28,28)
        )

        self.a3 = Inception(192,  64,  96, 128, 16, 32, 32)  # (1,192,28,28) -> (1,256,28,28)
        self.b3 = Inception(256, 128, 128, 192, 32, 96, 64)  # (1,256,28,28) -> (1,480,28,28)

        self.maxpool = nn.MaxPool2d(3, stride=2, padding=1)  # (1,480,28,28) -> (1,480,14,14)

        self.a4 = Inception(480, 192,  96, 208, 16,  48,  64)  # (1,480,14,14) -> (1,512,14,14)
        self.b4 = Inception(512, 160, 112, 224, 24,  64,  64)  # (1,512,14,14) -> (1,512,14,14)
        self.c4 = Inception(512, 128, 128, 256, 24,  64,  64)  # (1,512,14,14) -> (1,512,14,14)
        self.d4 = Inception(512, 112, 144, 288, 32,  64,  64)  # (1,512,14,14) -> (1,528,14,14)
        self.e4 = Inception(528, 256, 160, 320, 32, 128, 128)  # (1,528,14,14) -> (1,832,14,14)

        self.a5 = Inception(832, 256, 160, 320, 32, 128, 128)  # (1,832,7,7) -> (1,832,7,7)
        self.b5 = Inception(832, 384, 192, 384, 48, 128, 128)  # (1,832,7,7) -> (1,1024,7,7)

        self.avgpool = nn.AvgPool2d(7, stride=1)  # (1,1024,7,7) -> (1,1024,1,1)
        self.linear = nn.Linear(1024, 10)  # (1,1024) -> (1,10)

    def forward(self, x):
        out = self.pre_layers(x)
        out = self.a3(out)
        out = self.b3(out)
        out = self.maxpool(out)
        out = self.a4(out)
        out = self.b4(out)
        out = self.c4(out)
        out = self.d4(out)
        out = self.e4(out)
        out = self.maxpool(out)  # (1,832,14,14) -> (1,832,7,7)
        out = self.a5(out)
        out = self.b5(out)
        out = self.avgpool(out)
        out = out.view(out.size(0), -1)  # (1,1024,1,1) -> (1,1024)
        out = self.linear(out)
        return out
    
goog = GoogLeNet()

loss_func = nn.CrossEntropyLoss()
optimizer = torch.optim.Adam(goog.parameters(), lr=0.001)

running_loss = 0
for epoch, data in enumerate(trainloader):
    inputs, labels = data
    
    optimizer.zero_grad()
    
    out = goog(inputs)
    _, pred = out.max(1)
  
    loss = loss_func(out, labels)
    loss.backward()  # back propagation
    optimizer.step()
    
    running_loss += loss
    
    if (epoch+1)%100 == 0:
        print('epoch : %d, loss : %.3f [target : %d, pred : %d] ' %
                  (epoch + 1, running_loss/100, labels.numpy().item(), pred.numpy().item()))
        running_loss = 0  

#%% Accuracy (mnist)
def accuracy(mode):  # network를 다 학습시킨 다음, testset에서 정확도 구하기. Feedforward는 'F', Convolutional은 'C', GoogleNet은 'G'
    correct = 0
    for epoch, data in enumerate(testloader):
        inputs, labels = data
        
        if mode == 'F':
            inputs = inputs.view(1,28*28)
            out = net(inputs)
        elif mode == 'C':
            out = conv(inputs)
        elif mode == 'G':
            out = goog(inputs)
            
        _, pred = out.max(1)
        if labels == pred:
            correct += 1
        if (epoch+1)%1000 == 0:
            print('[Try : %d, Correct : %d]' % (epoch + 1,correct))
    print('accuracy : %.4f' % (correct/len(testloader)))

#%% Save and load the trained network
torch.save(net.state_dict(), "C:/Users/User/Desktop/업무/data/feed_cifar.pt")  # net부분에 내가 저장하길 원하는 모델 넣기
net.load_state_dict(torch.load("C:/Users/User/Desktop/업무/data/feed.pt"))  # 일단 틀을 만들어놓고 parameter들 가져와야 